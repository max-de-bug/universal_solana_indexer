use crate::idl::{AnchorIdl, IdlField, IdlType, IdlTypeDefTy};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::warn;

// ---------------------------------------------------------------------------
// Discriminator matching
// ---------------------------------------------------------------------------

/// Identify an instruction by matching the first 8 bytes against IDL discriminators.
pub fn match_instruction<'a>(
    data: &'a [u8],
    idl: &'a AnchorIdl,
) -> Option<(&'a crate::idl::IdlInstruction, &'a [u8])> {
    if data.len() < 8 {
        return None;
    }
    let disc = &data[..8];
    idl.instructions
        .iter()
        .find(|ix| ix.discriminator == disc)
        .map(|ix| (ix, &data[8..]))
}

/// Identify an account type by matching the first 8 bytes against IDL discriminators.
pub fn match_account<'a>(
    data: &'a [u8],
    idl: &'a AnchorIdl,
) -> Option<(&'a crate::idl::IdlAccountDef, &'a [u8])> {
    if data.len() < 8 {
        return None;
    }
    let disc = &data[..8];
    idl.accounts
        .iter()
        .find(|acc| acc.discriminator == disc)
        .map(|acc| (acc, &data[8..]))
}

// ---------------------------------------------------------------------------
// Dynamic Borsh decoder
// ---------------------------------------------------------------------------

/// Cursor-like reader over a byte slice for sequential Borsh decoding.
struct BorshReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BorshReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            anyhow::bail!(
                "Borsh read overflow: need {n} bytes at offset {}, but only {} remain",
                self.pos,
                self.remaining()
            );
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }
    fn read_bool(&mut self) -> anyhow::Result<bool> {
        Ok(self.read_u8()? != 0)
    }
    fn read_u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.read_bytes(2)?.try_into().unwrap()))
    }
    fn read_u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
    }
    fn read_u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
    }
    fn read_u128(&mut self) -> anyhow::Result<u128> {
        Ok(u128::from_le_bytes(
            self.read_bytes(16)?.try_into().unwrap(),
        ))
    }
    fn read_i8(&mut self) -> anyhow::Result<i8> {
        Ok(self.read_u8()? as i8)
    }
    fn read_i16(&mut self) -> anyhow::Result<i16> {
        Ok(self.read_u16()? as i16)
    }
    fn read_i32(&mut self) -> anyhow::Result<i32> {
        Ok(self.read_u32()? as i32)
    }
    fn read_i64(&mut self) -> anyhow::Result<i64> {
        Ok(self.read_u64()? as i64)
    }
    fn read_i128(&mut self) -> anyhow::Result<i128> {
        Ok(self.read_u128()? as i128)
    }
    fn read_f32(&mut self) -> anyhow::Result<f32> {
        Ok(f32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
    }
    fn read_f64(&mut self) -> anyhow::Result<f64> {
        Ok(f64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
    }

    fn read_string(&mut self) -> anyhow::Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    fn read_pubkey(&mut self) -> anyhow::Result<String> {
        let bytes = self.read_bytes(32)?;
        Ok(bs58::encode(bytes).into_string())
    }
}

/// Decode a list of fields from raw bytes into a JSON object.
pub fn decode_fields(
    data: &[u8],
    fields: &[IdlField],
    type_map: &HashMap<String, &crate::idl::IdlTypeDef>,
) -> anyhow::Result<Value> {
    let mut reader = BorshReader::new(data);
    let mut map = serde_json::Map::new();
    for field in fields {
        match decode_type(&mut reader, &field.field_type, type_map) {
            Ok(val) => {
                map.insert(field.name.clone(), val);
            }
            Err(e) => {
                warn!(field = %field.name, error = %e, "Skipping field, decode failed");
                map.insert(field.name.clone(), Value::Null);
                break; // subsequent fields would be at wrong offsets
            }
        }
    }
    Ok(Value::Object(map))
}

fn decode_type(
    reader: &mut BorshReader<'_>,
    ty: &IdlType,
    type_map: &HashMap<String, &crate::idl::IdlTypeDef>,
) -> anyhow::Result<Value> {
    match ty {
        IdlType::Primitive(p) => decode_primitive(reader, p),
        IdlType::Option { option } => {
            let tag = reader.read_u8()?;
            if tag == 0 {
                Ok(Value::Null)
            } else {
                decode_type(reader, option, type_map)
            }
        }
        IdlType::Vec { vec } => {
            let len = reader.read_u32()? as usize;
            let mut arr = Vec::with_capacity(len.min(10_000));
            for _ in 0..len {
                arr.push(decode_type(reader, vec, type_map)?);
            }
            Ok(Value::Array(arr))
        }
        IdlType::Array { array: (inner, n) } => {
            let mut arr = Vec::with_capacity(*n);
            for _ in 0..*n {
                arr.push(decode_type(reader, inner, type_map)?);
            }
            Ok(Value::Array(arr))
        }
        IdlType::Defined { defined } => {
            if let Some(typedef) = type_map.get(&defined.name) {
                match &typedef.type_def {
                    IdlTypeDefTy::Struct { fields } => {
                        let mut map = serde_json::Map::new();
                        for f in fields {
                            let val = decode_type(reader, &f.field_type, type_map)?;
                            map.insert(f.name.clone(), val);
                        }
                        Ok(Value::Object(map))
                    }
                    IdlTypeDefTy::Enum { variants } => {
                        let variant_idx = reader.read_u8()? as usize;
                        if variant_idx >= variants.len() {
                            return Ok(json!({ "variant": variant_idx }));
                        }
                        let variant = &variants[variant_idx];
                        if let Some(fields) = &variant.fields {
                            let mut map = serde_json::Map::new();
                            for f in fields {
                                let val = decode_type(reader, &f.field_type, type_map)?;
                                map.insert(f.name.clone(), val);
                            }
                            Ok(json!({ &variant.name: map }))
                        } else {
                            Ok(json!(&variant.name))
                        }
                    }
                }
            } else {
                warn!(type_name = %defined.name, "Unknown defined type, storing remaining as hex");
                Ok(json!(format!("<unknown:{}>", defined.name)))
            }
        }
    }
}

fn decode_primitive(reader: &mut BorshReader<'_>, name: &str) -> anyhow::Result<Value> {
    match name {
        "bool" => Ok(Value::Bool(reader.read_bool()?)),
        "u8" => Ok(json!(reader.read_u8()?)),
        "u16" => Ok(json!(reader.read_u16()?)),
        "u32" => Ok(json!(reader.read_u32()?)),
        "u64" => Ok(json!(reader.read_u64()?)),
        "u128" => Ok(json!(reader.read_u128()?.to_string())),
        "i8" => Ok(json!(reader.read_i8()?)),
        "i16" => Ok(json!(reader.read_i16()?)),
        "i32" => Ok(json!(reader.read_i32()?)),
        "i64" => Ok(json!(reader.read_i64()?)),
        "i128" => Ok(json!(reader.read_i128()?.to_string())),
        "f32" => Ok(json!(reader.read_f32()?)),
        "f64" => Ok(json!(reader.read_f64()?)),
        "string" => Ok(Value::String(reader.read_string()?)),
        "pubkey" | "publicKey" => Ok(Value::String(reader.read_pubkey()?)),
        "bytes" => {
            let len = reader.read_u32()? as usize;
            let bytes = reader.read_bytes(len)?;
            Ok(Value::String(bs58::encode(bytes).into_string()))
        }
        other => {
            warn!(%other, "Unknown primitive type");
            Ok(Value::Null)
        }
    }
}
