use rustc_hash::FxHashMap;

use crate::input::{data_lines, fields, lines};

pub struct People<'a> {
    ids: FxHashMap<&'a [u8], usize>,
    routes: Vec<(usize, u32)>,
}

struct Component<'a> {
    parent: usize,
    size: usize,
    name: &'a [u8],
}

fn root(nodes: &mut [Component<'_>], mut node: usize) -> usize {
    while nodes[node].parent != node {
        let grandparent = nodes[nodes[node].parent].parent;
        nodes[node].parent = grandparent;
        node = grandparent;
    }
    node
}

impl<'a> People<'a> {
    pub fn from_links(
        files: impl Iterator<Item = &'a [u8]>,
        mut intern_root: impl FnMut(&[u8]) -> (usize, u32),
    ) -> Self {
        let mut ids = FxHashMap::default();
        let mut nodes = Vec::<Component<'a>>::new();
        for [a, b] in files
            .flat_map(|file| lines(data_lines(file)))
            .filter_map(fields)
        {
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let [a, b] = [a, b].map(|name| {
                *ids.entry(name).or_insert_with(|| {
                    let index = nodes.len();
                    nodes.push(Component {
                        parent: index,
                        size: 1,
                        name,
                    });
                    index
                })
            });
            let mut left = root(&mut nodes, a);
            let mut right = root(&mut nodes, b);
            if left == right {
                continue;
            }
            if nodes[left].size < nodes[right].size {
                std::mem::swap(&mut left, &mut right);
            }
            nodes[right].parent = left;
            nodes[left].size += nodes[right].size;
            nodes[left].name = nodes[left].name.min(nodes[right].name);
        }
        let mut roots = vec![None; nodes.len()];
        let routes = (0..nodes.len())
            .map(|index| {
                let representative = root(&mut nodes, index);
                *roots[representative]
                    .get_or_insert_with(|| intern_root(nodes[representative].name))
            })
            .collect();
        Self { ids, routes }
    }

    #[inline]
    pub fn linked_route(&self, name: &[u8]) -> Option<(usize, u32)> {
        self.ids.get(name).map(|&index| self.routes[index])
    }
}

pub struct Products<'a> {
    products: FxHashMap<&'a [u8], u32>,
    pub categories: Vec<&'a [u8]>,
    pub unknown: u32,
}

impl<'a> Products<'a> {
    pub fn load(file: &'a [u8]) -> Self {
        let mut rows: Vec<[&[u8]; 2]> = lines(data_lines(file)).filter_map(fields).collect();
        let mut categories: Vec<_> = rows
            .iter()
            .map(|row| row[1])
            .chain(std::iter::once(b"unknown".as_slice()))
            .collect();
        categories.sort_unstable();
        categories.dedup();
        let index = |name| categories.binary_search(&name).unwrap() as u32;
        let unknown = index(b"unknown");
        let products = rows
            .drain(..)
            .map(|[product, name]| (product, index(name)))
            .collect();
        Self {
            products,
            categories,
            unknown,
        }
    }

    #[inline]
    pub fn category(&self, product: &[u8]) -> u32 {
        self.products.get(product).copied().unwrap_or(self.unknown)
    }
}

pub struct Campaigns<'a> {
    pub ids: FxHashMap<&'a [u8], u32>,
    pub names: Vec<[&'a [u8]; 2]>,
    pub organic: u32,
}

impl<'a> Campaigns<'a> {
    pub fn load(file: &'a [u8]) -> Self {
        let mut names: Vec<[&[u8]; 2]> = lines(data_lines(file)).filter_map(fields).collect();
        let ids = names
            .iter()
            .enumerate()
            .map(|(index, row)| (row[0], index as u32))
            .collect();
        let organic = names.len() as u32;
        names.push([b"organic", b"organic"]);
        Self {
            ids,
            names,
            organic,
        }
    }
}
