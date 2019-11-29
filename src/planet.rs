use crate::dat::Dat;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Write};

#[derive(
    Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Deserialize, Serialize,
)]
pub struct Region {
    name: String,
}

impl Region {
    /// Create a new `Region`.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Region { name: name.into() }
    }
}

impl fmt::Debug for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone)]
pub struct Planet {
    /// mapping from region A to a mapping from region B to the latency between
    /// A and B
    latencies: HashMap<Region, HashMap<Region, usize>>,
    /// mapping from each region to the regions sorted by distance
    sorted_by_distance: HashMap<Region, Vec<(usize, Region)>>,
}

impl Planet {
    /// Creates a new `Planet` instance.
    pub fn new(lat_dir: &str) -> Self {
        // create latencies
        let latencies: HashMap<_, _> = Dat::all_dats(lat_dir)
            .iter()
            .map(|dat| (dat.region(), dat.latencies()))
            .collect();

        // also create sorted by distance
        let sorted_by_distance = Self::sort_by_distance(latencies.clone());

        // return a new planet
        Planet {
            latencies,
            sorted_by_distance,
        }
    }

    /// Retrives a list with all regions.
    pub fn regions(&self) -> Vec<Region> {
        self.latencies.keys().cloned().collect()
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
    ) -> Option<&Vec<(usize, Region)>> {
        self.sorted_by_distance.get(from)
    }

    /// Returns a list of `Region`s sorted by the distance to the `Region`
    /// passed as argument.
    // TODO should this be here or simply expose `sorted_by_distance`?
    pub fn sorted_by_distance_and_indexed(
        &self,
        from: &Region,
    ) -> Option<HashMap<&Region, usize>> {
        // get sorted regions
        let sorted_regions = self.sorted_by_distance(from)?;

        // create a mapping from region to its sorted index
        let region_to_index = sorted_regions
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

    /// Returns a mapping from region to regions sorted by distance (ASC).
    fn sort_by_distance(
        latencies: HashMap<Region, HashMap<Region, usize>>,
    ) -> HashMap<Region, Vec<(usize, Region)>> {
        latencies
            .into_iter()
            .map(|(from, entries)| {
                // collect entries into a vector with reversed tuple order
                let mut entries: Vec<_> = entries
                    .into_iter()
                    .map(|(to, latency)| (latency, to))
                    .collect();

                // sort entries by latency
                entries.sort_unstable();

                (from, entries)
            })
            .collect()
    }
}

impl Planet {
    pub fn distance_matrix(
        &self,
        regions: Vec<Region>,
    ) -> Result<String, fmt::Error> {
        let mut output = String::new();

        // start header
        write!(&mut output, "| |")?;
        for r in regions.iter() {
            write!(&mut output, " {:?} |", r)?;
        }
        writeln!(&mut output, "")?;

        // end header
        write!(&mut output, "|:---:|")?;
        for _ in regions.iter() {
            write!(&mut output, ":---:|")?;
        }
        writeln!(&mut output, "")?;

        // for each region a
        for a in regions.iter() {
            write!(&mut output, "| __{:?}__ |", a)?;

            // compute latency from a to every other region b
            for b in regions.iter() {
                let lat = self.latency(a, b).unwrap();
                write!(&mut output, " {} |", lat)?;
            }
            writeln!(&mut output, "")?;
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use crate::planet::{Planet, Region};
    use std::collections::HashMap;

    // directory that contains all dat files
    const LAT_DIR: &str = "latency/";

    #[test]
    fn latency() {
        // planet
        let lat_dir = "latency/";
        let planet = Planet::new(lat_dir);

        // regions
        let eu_w3 = Region::new("europe-west3");
        let eu_w4 = Region::new("europe-west4");
        let us_c1 = Region::new("us-central1");

        assert_eq!(planet.latency(&eu_w3, &eu_w4).unwrap(), 7);

        // most times latency is symmetric
        assert_eq!(planet.latency(&eu_w3, &us_c1).unwrap(), 105);
        assert_eq!(planet.latency(&us_c1, &eu_w3).unwrap(), 105);
    }

    #[test]
    fn sorted_by_distance_and_indexed() {
        // planet
        let lat_dir = "latency/";
        let planet = Planet::new(lat_dir);

        // regions
        let eu_w3 = Region::new("europe-west3");

        // create expected regions:
        // - the first two have the same value, so they're ordered by name
        let expected = vec![
            Region::new("europe-west3"),
            Region::new("europe-west4"),
            Region::new("europe-west6"),
            Region::new("europe-west1"),
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
            Region::new("asia-northeast2"),
            Region::new("asia-east1"),
            Region::new("asia-east2"),
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

        assert_eq!(
            planet.sorted_by_distance_and_indexed(&eu_w3).unwrap(),
            expected
        );
    }

    #[test]
    fn distance_matrix() {
        let planet = Planet::new(LAT_DIR);
        let regions = vec![
            Region::new("asia-southeast1"),
            Region::new("europe-west4"),
            Region::new("southamerica-east1"),
            Region::new("australia-southeast1"),
            Region::new("europe-west2"),
            Region::new("asia-south1"),
            Region::new("us-east1"),
            Region::new("asia-northeast1"),
            Region::new("europe-west1"),
            Region::new("asia-east1"),
            Region::new("us-west1"),
            Region::new("europe-west3"),
            Region::new("us-central1"),
        ];
        assert!(planet.distance_matrix(regions).is_ok());
    }
}
