use crate::KvsEngine;
use crate::Result;
use sled::Db;

pub struct SledKvsEngine {
    db: Db,
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value: Option<String> = self
            .db
            .get(key.as_bytes())?
            .as_deref()
            .map(|inner| String::from_utf8_lossy(inner).into_owned());
        Ok(value)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }
}
