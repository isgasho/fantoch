use fantoch::planet::Region;
use fantoch_exp::Protocol;

pub struct PlotFmt;

impl PlotFmt {
    pub fn region_name(region: Region) -> &'static str {
        match region.name().as_str() {
            "ap-southeast-1" => "Singapore",
            "ca-central-1" => "Canada",
            "eu-west-1" => "Ireland",
            "sa-east-1" => "S. Paulo", // São Paulo
            "us-west-1" => "N. California", // Northern California
            name => {
                panic!("PlotFmt::region_name: name {} not supported!", name);
            }
        }
    }

    pub fn protocol_name(protocol: Protocol) -> &'static str {
        match protocol {
            Protocol::AtlasLocked => "Atlas",
            Protocol::EPaxosLocked => "EPaxos",
            Protocol::FPaxos => "FPaxos",
            Protocol::NewtAtomic => "Newt",
            Protocol::NewtLocked => "Newt-L",
            Protocol::Basic => "Basic",
        }
    }

    pub fn label(protocol: Protocol, f: usize) -> String {
        match protocol {
            Protocol::EPaxosLocked => Self::protocol_name(protocol).to_string(),
            _ => format!("{} f = {}", Self::protocol_name(protocol), f),
        }
    }

    pub fn color(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "#27ae60",
            (Protocol::AtlasLocked, 2) => "#16a085",
            // (Protocol::EPaxosLocked, _) => "#227093",
            (Protocol::EPaxosLocked, _) => "#444444",
            (Protocol::FPaxos, 1) => "#2980b9",
            (Protocol::FPaxos, 2) => "#34495e",
            (Protocol::NewtAtomic, 1) => "#f1c40f",
            (Protocol::NewtAtomic, 2) => "#e67e22",
            (Protocol::NewtLocked, 1) => "#3498db", // "#111111"
            (Protocol::NewtLocked, 2) => "#2980b9", // "#333333"
            (Protocol::Basic, _) => "#444444",
            _ => panic!(
                "PlotFmt::color: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    pub fn background_color(protocol: Protocol) -> String {
        match protocol {
            Protocol::AtlasLocked => "#ecf0f1",
            Protocol::FPaxos => "#95a5a6",
            Protocol::NewtAtomic => "#353b48",
            _ => panic!(
                "PlotFmt::background_color: protocol = {:?} not supported!",
                protocol
            ),
        }
        .to_string()
    }

    // Possible values: {'/', '\', '|', '-', '+', 'x', 'o', 'O', '.', '*'}
    pub fn hatch(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::FPaxos, 1) => "/", // 1
            (Protocol::FPaxos, 2) => "\\",
            (Protocol::EPaxosLocked, _) => "//", // 3
            (Protocol::AtlasLocked, 1) => "///", // 2
            (Protocol::AtlasLocked, 2) => "\\\\\\",
            (Protocol::NewtLocked, 1) => "////", // 4
            (Protocol::NewtLocked, 2) => "\\\\\\\\",
            (Protocol::NewtAtomic, 1) => "//////", //  6
            (Protocol::NewtAtomic, 2) => "\\\\\\\\\\\\",
            (Protocol::Basic, 1) => "///////", // 7
            (Protocol::Basic, 2) => "\\\\\\\\\\\\\\",
            _ => panic!(
                "PlotFmt::hatch: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    // Possible values: https://matplotlib.org/3.1.1/api/markers_api.html#module-matplotlib.markers
    pub fn marker(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "o",
            (Protocol::AtlasLocked, 2) => "s",
            (Protocol::EPaxosLocked, _) => "D",
            (Protocol::FPaxos, 1) => "+",
            (Protocol::FPaxos, 2) => "x",
            (Protocol::NewtAtomic, 1) => "v",
            (Protocol::NewtAtomic, 2) => "^",
            (Protocol::NewtLocked, 1) => ">",
            (Protocol::NewtLocked, 2) => "<",
            (Protocol::Basic, 1) => "p",
            (Protocol::Basic, 2) => "P",
            _ => panic!(
                "PlotFmt::marker: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }.to_string()
    }

    // Possible values:  {'-', '--', '-.', ':', ''}
    pub fn linestyle(protocol: Protocol, f: usize) -> String {
        match (protocol, f) {
            (Protocol::AtlasLocked, _) => "--",
            (Protocol::EPaxosLocked, _) => ":",
            (Protocol::FPaxos, _) => "-.",
            (Protocol::NewtAtomic, _) => "-",
            (Protocol::NewtLocked, _) => "-",
            (Protocol::Basic, _) => ":",
        }
        .to_string()
    }

    pub fn linewidth(f: usize) -> String {
        match f {
            1 => 1.5,
            2 => 2.0,
            _ => panic!("PlotFmt::linewidth: f = {} not supported!", f),
        }
        .to_string()
    }
}
