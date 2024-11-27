use std::path::Path;

pub struct SledStore;

impl SledStore {
    pub fn open(_path: &Path) -> super::Result<Self> {
        unimplemented!()
    }
}

impl super::KvsEngine for SledStore {
    fn set(&mut self, _key: String, _value: String) -> super::Result<()> {
        unimplemented!()
    }

    fn get(&mut self, _key: String) -> super::Result<Option<String>> {
        unimplemented!()
    }

    fn remove(&mut self, _key: String) -> super::Result<()> {
        unimplemented!()
    }
}
