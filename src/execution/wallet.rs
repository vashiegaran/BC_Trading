use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;

/// Secure wrapper around a Solana keypair.
///
/// **SAFETY Rule 1:** This struct intentionally does NOT implement `Debug`
/// or `Display` so the private key can never be leaked through logging,
/// error messages, or format strings.
pub struct BotWallet {
    keypair: Keypair,
}

// Explicitly ensure no Debug/Display — the compiler enforces this because
// we simply don't derive or implement them.  Adding a manual impl that
// redacts the key makes the intent crystal clear.
impl std::fmt::Debug for BotWallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BotWallet")
            .field("pubkey", &self.keypair.pubkey().to_string())
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

impl BotWallet {
    /// Load a wallet from the `WALLET_PRIVATE_KEY` environment variable.
    ///
    /// The key must be a base58-encoded 64-byte Solana keypair.
    /// This is the **only** place the raw private key is decoded.
    pub fn from_env(wallet_private_key: &str) -> Result<Self> {
        let key_bytes = bs58::decode(wallet_private_key)
            .into_vec()
            .context("Failed to base58-decode WALLET_PRIVATE_KEY")?;

        let keypair = Keypair::from_bytes(&key_bytes)
            .context("Failed to create Keypair from decoded bytes (expected 64 bytes)")?;

        // Drop raw bytes immediately — only the Keypair struct survives.
        drop(key_bytes);

        Ok(Self { keypair })
    }

    /// The public key (wallet address). Safe to log and display.
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Sign a versioned transaction.
    ///
    /// Currently a stub — will be completed in Phase 5 for real
    /// transaction signing. Returns the keypair signature over
    /// the transaction's message bytes.
    pub fn sign_transaction(&self, tx: &VersionedTransaction) -> Result<Signature> {
        use solana_sdk::signer::Signer;
        let message_bytes = tx.message.serialize();
        let signature = self.keypair.sign_message(&message_bytes);
        Ok(signature)
    }

    /// Expose a reference to the inner keypair for Solana SDK operations
    /// that require `&Keypair` (e.g. signing).  Keep usage minimal.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }
}
