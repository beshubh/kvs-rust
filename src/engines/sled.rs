use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub struct SledStore(Arc<Mutex<SharedSledStore>>);

pub struct SharedSledStore {}

impl SledStore {
    pub fn open(_path: &Path) -> super::Result<Self> {
        unimplemented!()
    }
}

impl super::KvsEngine for SledStore {
    fn set(&self, _key: String, _value: String) -> super::Result<()> {
        unimplemented!()
    }

    fn get(&self, _key: String) -> super::Result<Option<String>> {
        unimplemented!()
    }

    fn remove(&self, _key: String) -> super::Result<()> {
        unimplemented!()
    }
}

impl Clone for SledStore {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}
