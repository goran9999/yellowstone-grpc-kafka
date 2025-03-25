use {
    super::config::KafkaDedupImpl,
    deadpool_redis::redis::{self, AsyncCommands},
    std::{
        collections::{btree_map::Entry, BTreeMap, HashSet},
        sync::Arc,
    },
    tokio::sync::Mutex,
};

#[async_trait::async_trait]
pub trait KafkaDedup: Clone {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct KafkaDedupMemory {
    inner: Arc<Mutex<BTreeMap<u64, HashSet<[u8; 32]>>>>,
}

#[derive(Clone)]
pub struct KafkaDedupRedis {
    pub pool: deadpool_redis::Pool,
}

#[async_trait::async_trait]
impl KafkaDedup for KafkaDedupMemory {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool {
        let mut map = self.inner.lock().await;

        if let Some(key_slot) = map.keys().next().cloned() {
            if slot < key_slot {
                return false;
            }
        }

        match map.entry(slot) {
            Entry::Vacant(entry) => {
                entry.insert(HashSet::new()).insert(hash);

                // remove old sets, keep ~30sec log
                while let Some(key_slot) = map.keys().next().cloned() {
                    if key_slot < slot - 75 {
                        map.remove(&key_slot);
                    } else {
                        break;
                    }
                }

                true
            }
            Entry::Occupied(entry) => entry.into_mut().insert(hash),
        }
    }
}

#[async_trait::async_trait]
impl KafkaDedup for KafkaDedupImpl {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool {
        match self {
            Self::Memory(dedup) => dedup.allowed(slot, hash).await,
            Self::Redis(dedup) => dedup.allowed(slot, hash).await,
        }
    }
}

#[async_trait::async_trait]
impl KafkaDedup for KafkaDedupRedis {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool {
        (self.is_allowed(slot, hash).await).unwrap_or(false)
    }
}

impl KafkaDedupRedis {
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let pool =
            deadpool_redis::Pool::builder(deadpool_redis::Manager::new(redis_url)?).build()?;

        Ok(Self { pool })
    }

    pub async fn is_allowed(&self, slot: u64, hash: [u8; 32]) -> anyhow::Result<bool> {
        let mut conn = self.pool.get().await?;

        let first_slots: Vec<String> = conn.zrange("slots", 0, 0).await?;
        if let Some(first_slot_str) = first_slots.first() {
            let first_slot: u64 = first_slot_str.parse()?;
            if slot < first_slot {
                return Ok(false);
            }
        }

        let hash_str = hex::encode(hash);

        let slot_key = format!("slot:{}", slot);

        let _: () = conn.zadd("slots", slot, slot).await?;

        let is_new: i32 = conn.hset_nx(&slot_key, &hash_str, 1).await?;

        if is_new == 1 {
            let cutoff_slot = if slot > 75 { slot - 75 } else { 0 };
            let old_slots: Vec<String> = conn.zrangebyscore("slots", 0, cutoff_slot).await?;

            if !old_slots.is_empty() {
                let mut cleanup_pipe = redis::pipe();

                for old_slot in old_slots {
                    cleanup_pipe.del(format!("slot:{}", old_slot)).ignore();
                }

                cleanup_pipe
                    .cmd("ZREMRANGEBYSCORE")
                    .arg("slots")
                    .arg(0)
                    .arg(cutoff_slot)
                    .ignore();

                let _: () = cleanup_pipe.query_async(&mut conn).await?;
            }
        }

        Ok(is_new == 1)
    }
}
