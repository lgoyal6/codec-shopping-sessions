use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::input::{DAY, WEEK};
use crate::records::{Click, Event, group_by_index};

const SESSION_GAP: i64 = 30 * 60_000;

pub struct UserDay<'a> {
    pub person: &'a [u8],
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub category: Option<u32>,
}

#[derive(Default)]
pub struct CampaignDay {
    pub sessions: u64,
    pub purchases: u64,
    pub revenue: u64,
    pub assists: u64,
}

impl CampaignDay {
    pub fn merge(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchases += other.purchases;
        self.revenue += other.revenue;
        self.assists += other.assists;
    }
}

pub type CampaignTotals = FxHashMap<(u32, i32), CampaignDay>;

#[derive(Clone, Copy)]
struct Session {
    start: i64,
    revenue: u64,
}

#[inline]
fn day(time: i64) -> i32 {
    time.div_euclid(DAY) as i32
}

struct DayTotals {
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
}

#[derive(Default)]
struct CategoryTotals {
    sums: Vec<u64>,
    used: Vec<u32>,
}

impl CategoryTotals {
    fn add(&mut self, category: u32, revenue: u64) {
        let sum = &mut self.sums[category as usize];
        if *sum == 0 {
            self.used.push(category);
        }
        *sum += revenue;
    }

    fn best_and_clear(&mut self) -> Option<u32> {
        let best = self.used.iter().copied().max_by(|&a, &b| {
            self.sums[a as usize]
                .cmp(&self.sums[b as usize])
                .then_with(|| b.cmp(&a))
        });
        for category in self.used.drain(..) {
            self.sums[category as usize] = 0;
        }
        best
    }
}

impl DayTotals {
    fn new(day: i32) -> Self {
        Self {
            day,
            sessions: 0,
            events: 0,
            revenue: 0,
        }
    }

    fn finish<'a>(&self, person: &'a [u8], categories: &mut CategoryTotals) -> UserDay<'a> {
        UserDay {
            person,
            day: self.day,
            sessions: self.sessions,
            events: self.events,
            revenue: self.revenue,
            category: categories.best_and_clear(),
        }
    }
}

fn sessionize<'a>(
    person: &'a [u8],
    events: &[Event],
    sessions: &mut Vec<Session>,
    days: &mut Vec<UserDay<'a>>,
    categories: &mut CategoryTotals,
) {
    let first = events[0];
    let mut last = first.time;
    let mut session = Session {
        start: first.time,
        revenue: 0,
    };
    let mut totals = DayTotals::new(day(first.time));
    sessions.clear();

    for event in events {
        if event.time - last > SESSION_GAP {
            sessions.push(session);
            totals.sessions += 1;
            let new_day = day(event.time);
            if new_day != totals.day {
                days.push(totals.finish(person, categories));
                totals = DayTotals::new(new_day);
            }
            session = Session {
                start: event.time,
                revenue: 0,
            };
        }
        last = event.time;
        let amount = u64::from(event.amount);
        session.revenue += amount;
        totals.events += 1;
        totals.revenue += amount;
        if amount != 0 {
            categories.add(event.category, amount);
        }
    }
    sessions.push(session);
    totals.sessions += 1;
    days.push(totals.finish(person, categories));
}

#[derive(Clone, Copy)]
struct Interval {
    first: i64,
    last: i64,
}

fn add_assists(
    campaign: u32,
    interval: Interval,
    sessions: &[Session],
    totals: &mut CampaignTotals,
) {
    let mut start = sessions.partition_point(|session| session.start < interval.first);
    let stop = start + sessions[start..].partition_point(|session| session.start <= interval.last);
    while start < stop {
        let date = day(sessions[start].start);
        let end_of_day = (i64::from(date) + 1) * DAY;
        let end =
            start + sessions[start..stop].partition_point(|session| session.start < end_of_day);
        totals.entry((campaign, date)).or_default().assists += (end - start) as u64;
        start = end;
    }
}

struct Attribution {
    organic: u32,
    intervals: Vec<Option<Interval>>,
    used: Vec<u32>,
}

impl Attribution {
    fn new(campaign_count: usize, organic: u32) -> Self {
        Self {
            organic,
            intervals: vec![None; campaign_count],
            used: Vec::new(),
        }
    }

    fn credit(&mut self, sessions: &[Session], clicks: &[Click<'_>], totals: &mut CampaignTotals) {
        let mut next = 0;
        let mut most_recent: Option<Click<'_>> = None;
        for session in sessions {
            while next < clicks.len() && clicks[next].time <= session.start {
                let click = clicks[next];
                if most_recent.is_none_or(|prior| click.time > prior.time) {
                    most_recent = Some(click);
                }
                next += 1;
            }
            let campaign = most_recent
                .filter(|click| session.start - click.time <= WEEK)
                .map_or(self.organic, |click| click.campaign);
            let day_total = totals.entry((campaign, day(session.start))).or_default();
            day_total.sessions += 1;
            day_total.purchases += u64::from(session.revenue > 0);
            day_total.revenue += session.revenue;
        }

        let earliest = sessions[0].start - WEEK;
        for click in &clicks[..next] {
            if click.time < earliest {
                continue;
            }
            let slot = &mut self.intervals[click.campaign as usize];
            if let Some(interval) = slot {
                if click.time <= interval.last {
                    interval.last = click.time.saturating_add(WEEK);
                    continue;
                }
                add_assists(click.campaign, *interval, sessions, totals);
            } else {
                self.used.push(click.campaign);
            }
            *slot = Some(Interval {
                first: click.time,
                last: click.time.saturating_add(WEEK),
            });
        }

        for campaign in self.used.drain(..) {
            let interval = self.intervals[campaign as usize].take().unwrap();
            add_assists(campaign, interval, sessions, totals);
        }
    }
}

pub fn summarize<'a>(
    event_pieces: Vec<Vec<Event>>,
    click_pieces: Vec<Vec<Click<'_>>>,
    names: &'a [Arc<[u8]>],
    category_count: usize,
    campaign_count: usize,
    organic: u32,
) -> (Vec<UserDay<'a>>, CampaignTotals) {
    let mut events = Vec::with_capacity(event_pieces.iter().map(Vec::len).sum());
    let mut event_counts = vec![0; names.len()];
    for event in event_pieces.into_iter().flatten() {
        event_counts[event.person as usize] += 1;
        events.push(event);
    }

    let mut clicks = Vec::with_capacity(click_pieces.iter().map(Vec::len).sum());
    let mut click_counts = vec![0; names.len()];
    for click in click_pieces.into_iter().flatten() {
        click_counts[click.person as usize] += 1;
        clicks.push(click);
    }

    let event_ends = group_by_index(&mut events, &event_counts, |row| row.person as usize);
    let click_ends = group_by_index(&mut clicks, &click_counts, |row| row.person as usize);

    let mut days = Vec::new();
    let mut campaign_totals = CampaignTotals::default();
    let mut attribution = Attribution::new(campaign_count, organic);
    let mut categories = CategoryTotals {
        sums: vec![0; category_count],
        used: Vec::new(),
    };
    let mut sessions = Vec::new();
    let mut event_start = 0;
    let mut click_start = 0;
    for ((name, event_end), click_end) in names.iter().zip(event_ends).zip(click_ends) {
        let person_events = &mut events[event_start..event_end];
        let person_clicks = &mut clicks[click_start..click_end];
        person_events.sort_unstable_by_key(|event| event.time);
        person_clicks.sort_unstable_by(|a, b| a.time.cmp(&b.time).then_with(|| a.id.cmp(b.id)));
        sessionize(
            name,
            person_events,
            &mut sessions,
            &mut days,
            &mut categories,
        );
        attribution.credit(&sessions, person_clicks, &mut campaign_totals);
        event_start = event_end;
        click_start = click_end;
    }
    (days, campaign_totals)
}
