use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use tracing::info;

// ---------------------------------------------------------------------------
// Anchor IDL data model (supports both v0.29 legacy and v0.30+ formats)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorIdl {
    #[serde(default)]
    pub address: Option<String>,
    pub metadata: IdlMetadata,
    pub instructions: Vec<IdlInstruction>,
    #[serde(default)]
    pub accounts: Vec<IdlAccountDef>,
    #[serde(default)]
    pub types: Vec<IdlTypeDef>,
    #[serde(default)]
    pub events: Vec<IdlEvent>,
    #[serde(default)]
    pub errors: Vec<IdlErrorDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlMetadata {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstruction {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
    #[serde(default)]
    pub accounts: Vec<IdlInstructionAccount>,
    #[serde(default)]
    pub args: Vec<IdlField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstructionAccount {
    pub name: String,
    #[serde(default)]
    pub writable: bool,
    #[serde(default)]
    pub signer: bool,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlAccountDef {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlTypeDef {
    pub name: String,
    #[serde(rename = "type")]
    pub type_def: IdlTypeDefTy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum IdlTypeDefTy {
    #[serde(rename = "struct")]
    Struct {
        #[serde(default)]
        fields: Vec<IdlField>,
    },
    #[serde(rename = "enum")]
    Enum {
        #[serde(default)]
        variants: Vec<IdlEnumVariant>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: IdlType,
}

/// Represents every Anchor IDL type variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdlType {
    Option {
        option: Box<IdlType>,
    },
    Vec {
        vec: Box<IdlType>,
    },
    Array {
        array: (Box<IdlType>, usize),
    },
    Defined {
        defined: IdlDefinedRef,
    },
    /// Leaf primitives: "u8", "bool", "string", "pubkey", etc.
    Primitive(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlDefinedRef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlEnumVariant {
    pub name: String,
    #[serde(default)]
    pub fields: Option<Vec<IdlField>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlEvent {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlErrorDef {
    pub code: u32,
    pub name: String,
    #[serde(default)]
    pub msg: Option<String>,
}

// ---------------------------------------------------------------------------
// IDL loading & helpers
// ---------------------------------------------------------------------------

impl AnchorIdl {
    /// Load and parse an Anchor IDL JSON file from disk.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read IDL at {path}: {e}"))?;
        let mut idl: Self = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse IDL JSON: {e}"))?;

        // Back-fill discriminators for IDLs that do not embed them (legacy).
        for ix in &mut idl.instructions {
            if ix.discriminator.is_empty() {
                ix.discriminator = compute_instruction_discriminator(&ix.name);
            }
        }
        for acc in &mut idl.accounts {
            if acc.discriminator.is_empty() {
                acc.discriminator = compute_account_discriminator(&acc.name);
            }
        }

        idl.log_summary();
        Ok(idl)
    }

    /// Fetch an IDL from an on-chain Anchor IDL account, decompress, and parse.
    pub async fn from_chain(
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
        idl_account: &solana_sdk::pubkey::Pubkey,
    ) -> anyhow::Result<Self> {
        use flate2::read::ZlibDecoder;

        info!(%idl_account, "Fetching IDL from on-chain account");

        let account_data = rpc
            .get_account_data(idl_account)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch IDL account: {e}"))?;

        // Anchor IDL account layout:
        //   [8B discriminator] [32B authority] [4B data_len LE] [compressed data...]
        anyhow::ensure!(account_data.len() > 44, "IDL account data too short");

        let data_len = u32::from_le_bytes(account_data[40..44].try_into()?) as usize;
        let compressed = &account_data[44..44 + data_len.min(account_data.len() - 44)];

        let mut decoder = ZlibDecoder::new(compressed);
        let mut json_str = String::new();
        decoder
            .read_to_string(&mut json_str)
            .map_err(|e| anyhow::anyhow!("Failed to decompress IDL: {e}"))?;

        info!(json_bytes = json_str.len(), "IDL decompressed from chain");

        let mut idl: Self = serde_json::from_str(&json_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse on-chain IDL JSON: {e}"))?;

        for ix in &mut idl.instructions {
            if ix.discriminator.is_empty() {
                ix.discriminator = compute_instruction_discriminator(&ix.name);
            }
        }
        for acc in &mut idl.accounts {
            if acc.discriminator.is_empty() {
                acc.discriminator = compute_account_discriminator(&acc.name);
            }
        }

        idl.log_summary();
        Ok(idl)
    }

    fn log_summary(&self) {
        info!(
            name = %self.metadata.name,
            instructions = self.instructions.len(),
            accounts = self.accounts.len(),
            types = self.types.len(),
            "Anchor IDL loaded"
        );
    }

    /// Build a lookup of type definitions by name.
    pub fn type_map(&self) -> HashMap<String, &IdlTypeDef> {
        self.types.iter().map(|t| (t.name.clone(), t)).collect()
    }

    /// Resolve the struct fields for a given account name.
    pub fn account_fields(&self, account_name: &str) -> Option<&Vec<IdlField>> {
        self.types
            .iter()
            .find(|t| t.name == account_name)
            .and_then(|t| match &t.type_def {
                IdlTypeDefTy::Struct { fields } => Some(fields),
                _ => None,
            })
    }
}

/// Anchor instruction discriminator: first 8 bytes of SHA-256("global:{name}").
fn compute_instruction_discriminator(name: &str) -> Vec<u8> {
    let preimage = format!("global:{}", to_snake_case(name));
    let hash = Sha256::digest(preimage.as_bytes());
    hash[..8].to_vec()
}

/// Anchor account discriminator: first 8 bytes of SHA-256("account:{Name}").
fn compute_account_discriminator(name: &str) -> Vec<u8> {
    let preimage = format!("account:{name}");
    let hash = Sha256::digest(preimage.as_bytes());
    hash[..8].to_vec()
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// SQL type mapping
// ---------------------------------------------------------------------------

/// Map an IDL type to its closest PostgreSQL column type.
pub fn idl_type_to_sql(ty: &IdlType) -> &'static str {
    match ty {
        IdlType::Primitive(p) => match p.as_str() {
            "bool" => "BOOLEAN",
            "u8" | "i8" | "u16" | "i16" => "SMALLINT",
            "u32" | "i32" => "INTEGER",
            "u64" | "i64" => "BIGINT",
            "u128" | "i128" => "NUMERIC(39,0)",
            "f32" => "REAL",
            "f64" => "DOUBLE PRECISION",
            "string" => "TEXT",
            "pubkey" | "publicKey" => "TEXT",
            "bytes" => "BYTEA",
            _ => "JSONB",
        },
        IdlType::Option { option } => idl_type_to_sql(option),
        IdlType::Vec { .. } | IdlType::Array { .. } | IdlType::Defined { .. } => "JSONB",
    }
}
