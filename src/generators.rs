use rand::distributions::Uniform;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

pub trait RowGenerator {
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self
    where
        Self: Sized;
    fn get_csv_header(&self) -> String;
    fn get_csv_row(&mut self) -> String;
}

pub struct GroupByGenerator {
    nas: u8,
    distr_k: Uniform<u64>,
    distr_nk: Uniform<u64>,
    distr_5: Uniform<u8>,
    distr_15: Uniform<u8>,
    distr_float: Uniform<f64>,
    distr_nas: Uniform<u8>,
    rng: ChaCha8Rng,
}

pub struct JoinGeneratorBig {
    nas: u8,
    keys_1: Vec<u64>,
    keys_2: Vec<u64>,
    keys_3: Vec<u64>,
    distr_float: Uniform<f64>,
    distr_nas: Uniform<u8>,
    rng: ChaCha8Rng,
}

pub struct JoinGeneratorMedium {
    keys_1: Vec<u64>,
    keys_2: Vec<u64>,
    distr_float: Uniform<f64>,
    rng: ChaCha8Rng,
}

pub struct JoinGeneratorSmall {
    keys_1: Vec<u64>,
    distr_float: Uniform<f64>,
    rng: ChaCha8Rng,
}

impl RowGenerator for GroupByGenerator {
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self {
        GroupByGenerator {
            nas,
            distr_k: Uniform::<u64>::try_from(1..=k).unwrap(),
            distr_nk: Uniform::<u64>::try_from(1..=(n / k)).unwrap(),
            distr_5: Uniform::<u8>::try_from(1..=5).unwrap(),
            distr_15: Uniform::<u8>::try_from(1..=15).unwrap(),
            distr_float: Uniform::<f64>::try_from(0.0..=100.0).unwrap(),
            distr_nas: Uniform::<u8>::try_from(0..=100).unwrap(),
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }
    fn get_csv_header(&self) -> String {
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n".to_string()
    }

    fn get_csv_row(&mut self) -> String {
        format!(
            "{},{},{},{},{},{},{:.5},{:.5},{:.5}\n",
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:03}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:03}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:010}", self.distr_nk.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_nk.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            self.distr_5.sample(&mut self.rng),
            self.distr_15.sample(&mut self.rng),
            self.distr_float.sample(&mut self.rng),
        )
    }
}

impl RowGenerator for JoinGeneratorBig {
    #[allow(unused_variables)]
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut k1: Vec<u64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original line: key1 = split_xlr(N/1e6)
        k1.shuffle(&mut rng);

        let mut k2: Vec<u64> = (1..=(n * 11 / 10 / 1_000)).collect(); // original line: key2 = split_xlr(N/1e3)
        k2.shuffle(&mut rng);

        let mut k3: Vec<u64> = (1..=(n * 11 / 10)).collect(); // original line: key3 = split_xlr(N)
        k3.shuffle(&mut rng);

        // orginial line (43:44)
        // x = key[seq.int(1, n*0.9)],
        // l = key[seq.int(n*0.9+1, n)],
        // Sampling both (x, l) is equal to sampling from 1..n
        // where n = n / 1e6
        k1 = k1
            .get(0..(n as usize / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();

        // same logic here
        // see https://github.com/h2oai/db-benchmark/blob/master/_data/join-datagen.R#L40
        // for details
        k2 = k2
            .get(0..(n as usize / 1_000))
            .expect("internal indexing error with k2")
            .to_vec();
        k3 = k3
            .get(0..(n as usize))
            .expect("internal indexing error with k3")
            .to_vec();

        JoinGeneratorBig {
            nas,
            keys_1: k1,
            keys_2: k2,
            keys_3: k3,
            distr_float: Uniform::try_from(0.0..=100.0).unwrap(),
            distr_nas: Uniform::<u8>::try_from(0..=100).unwrap(),
            rng,
        }
    }

    fn get_csv_header(&self) -> String {
        "id1,id2,id3,id4,id5,id6,v2\n".to_string()
    }

    fn get_csv_row(&mut self) -> String {
        let k1 = self.keys_1.choose(&mut self.rng).unwrap();
        let k2 = self.keys_2.choose(&mut self.rng).unwrap();
        let k3 = self.keys_3.choose(&mut self.rng).unwrap();
        format!(
            "{},{},{},id{},id{},id{},{:.5}\n",
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", k1)
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", k2)
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", k3)
            } else {
                "".to_string()
            },
            k1,
            k2,
            k3,
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_float.sample(&mut self.rng))
            } else {
                "".to_string()
            },
        )
    }
}

impl RowGenerator for JoinGeneratorSmall {
    #[allow(unused_variables)]
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut k1: Vec<u64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original line: key1 = split_xlr(N/1e6)
        k1.shuffle(&mut rng);

        // orginial line (43:44)
        // x = key[seq.int(1, n*0.9)],
        // l = key[seq.int(n*0.9+1, n)],
        // r = key[seq.int(n+1, n*1.1)]
        // we need (x, r) here
        let mut kx = k1
            .get(0..(n as usize * 9 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();
        let mut kr = k1
            .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();

        kx.append(&mut kr);

        JoinGeneratorSmall {
            keys_1: kx,
            distr_float: Uniform::try_from(1.0..=100.0).unwrap(),
            rng,
        }
    }

    fn get_csv_header(&self) -> String {
        "id1,id4,v2\n".to_string()
    }

    fn get_csv_row(&mut self) -> String {
        let k1 = self.keys_1.choose(&mut self.rng).unwrap();
        format!(
            "{},id{},{:.5}\n",
            k1,
            k1,
            self.distr_float.sample(&mut self.rng),
        )
    }
}

impl RowGenerator for JoinGeneratorMedium {
    #[allow(unused_variables)]
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut k1: Vec<u64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original line: key1 = split_xlr(N/1e6)
        k1.shuffle(&mut rng);

        let mut k2: Vec<u64> = (1..=(n * 11 / 10 / 1_000)).collect(); // original line: key2 = split_xlr(N/1e3)
        k2.shuffle(&mut rng);

        // orginial line (43:44)
        // x = key[seq.int(1, n*0.9)],
        // l = key[seq.int(n*0.9+1, n)],
        // Sampling both (x, l) is equal to sampling from 1..n
        // where n = n / 1e6
        let mut k1x = k1
            .get(0..(n as usize * 9 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();
        let mut k1r = k1
            .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();

        k1x.append(&mut k1r);

        let mut k2x = k2
            .get(0..(n as usize * 9 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();
        let mut k2r = k2
            .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
            .expect("internal indexing error with k1")
            .to_vec();

        k2x.append(&mut k2r);

        JoinGeneratorMedium {
            keys_1: k1x,
            keys_2: k2x,
            distr_float: Uniform::try_from(1.0..=100.0).unwrap(),
            rng,
        }
    }

    fn get_csv_header(&self) -> String {
        "id1,id2,id4,id5,v2\n".to_string()
    }

    fn get_csv_row(&mut self) -> String {
        let k1 = self.keys_1.choose(&mut self.rng).unwrap();
        let k2 = self.keys_2.choose(&mut self.rng).unwrap();
        format!(
            "{},{},id{},id{},{:.5}\n",
            k1,
            k2,
            k1,
            k2,
            self.distr_float.sample(&mut self.rng),
        )
    }
}
