use crate::dat::Dat;
use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Region {
    name: String,
}

impl Region {
    /// Create a new `Region`.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Region { name: name.into() }
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub struct Planet {
    /// mapping from region A to a mapping from region B to the latency between
    /// A and B
    latencies: HashMap<Region, HashMap<Region, usize>>,
}

impl Planet {
    /// Creates a new `Planet` instance.
    pub fn new(lat_dir: &str) -> Self {
        // create latencies
        let latencies = Dat::all_dats(lat_dir)
            .iter()
            .map(|dat| (dat.region(), dat.latencies()))
            .collect();

        // return a new planet
        Planet { latencies }
    }

    /// Retrives the distance between the two regions passed as argument.
    pub fn latency(&self, from: &Region, to: &Region) -> Option<usize> {
        // get from's entries
        let entries = self.latencies.get(from)?;

        // get to's entry in from's entries
        entries.get(to).cloned()
    }

    /// Returns a list of `Region`s sorted by the distance to the `Region`
    /// passed as argument.
    pub fn sorted_by_distance(
        &self,
        from: &Region,
    ) -> Option<HashMap<&Region, usize>> {
        // get from's entries
        let entries = self.latencies.get(from)?;

        // collect entries into a vector with reversed tuple order
        let mut entries: Vec<_> =
            entries.iter().map(|(to, latency)| (latency, to)).collect();

        // now latency first appears first, so we can sort entries by latency
        entries.sort_unstable();

        // create a mapping from region to its sorted index
        let region_to_index = entries
            .into_iter()
            // drop latencies
            .map(|(_, to)| to)
            .enumerate()
            // reverse: now regions map to sort index
            .map(|(index, to)| (to, index))
            .collect();

        // return region to index mapping
        Some(region_to_index)
    }
}

#[cfg(test)]
mod tests {
    use crate::planet::{Planet, Region};
    use std::collections::HashMap;

    #[test]
    fn latency() {
        // planet
        let lat_dir = "latency/";
        let planet = Planet::new(lat_dir);

        // regions
        let eu_w3 = Region::new("europe-west3");
        let eu_w4 = Region::new("europe-west4");
        let us_c1 = Region::new("us-central1");
        let au_s1 = Region::new("australia-southeast1");

        assert_eq!(planet.latency(&eu_w3, &eu_w4).unwrap(), 7);

        // sometimes latency is symmetric
        assert_eq!(planet.latency(&eu_w3, &us_c1).unwrap(), 109);
        assert_eq!(planet.latency(&us_c1, &eu_w3).unwrap(), 109);

        // sometimes it's not
        assert_eq!(planet.latency(&eu_w3, &au_s1).unwrap(), 281);
        assert_eq!(planet.latency(&au_s1, &eu_w3).unwrap(), 282);
    }

    #[test]
    fn sorted_by_distance() {
        // planet
        let lat_dir = "latency/";
        let planet = Planet::new(lat_dir);

        // regions
        let eu_w3 = Region::new("europe-west3");

        // create expected regions:
        // - the first two have the same value, so they're ordered by name
        let expected = vec![
            Region::new("europe-west3"),
            Region::new("europe-west1"),
            Region::new("europe-west4"),
            Region::new("europe-west2"),
            Region::new("europe-north1"),
            Region::new("us-east4"),
            Region::new("northamerica-northeast1"),
            Region::new("us-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
            Region::new("us-west2"),
            Region::new("southamerica-east1"),
            Region::new("asia-northeast1"),
            Region::new("asia-east1"),
            Region::new("australia-southeast1"),
            Region::new("asia-southeast1"),
            Region::new("asia-south1"),
        ];
        // create a mapping from region to its sorted index
        let expected: HashMap<_, _> = expected
            .iter()
            .enumerate()
            .map(|(index, region)| (region, index))
            .collect();

        assert_eq!(planet.sorted_by_distance(&eu_w3).unwrap(), expected);
    }
}