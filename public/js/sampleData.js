const DAYS_AHEAD = [5, 7, 12];

function formatDate(offset) {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() + offset);
  return d.toISOString().split('T')[0];
}

function deepFreeze(value) {
  if (Array.isArray(value)) {
    value.forEach(deepFreeze);
    return Object.freeze(value);
  }
  if (value && typeof value === 'object') {
    Object.keys(value).forEach(key => deepFreeze(value[key]));
    return Object.freeze(value);
  }
  return value;
}

const rawDecisions = [
  {
    id: 'sample-decision-1',
    type: 'goal',
    text: 'Explore this Application',
    completed: false,
    parentGoalId: null,
    scheduled: formatDate(DAYS_AHEAD[1]),
    scheduledEnd: formatDate(DAYS_AHEAD[2]),
    hiddenUntil: null,
    tags: [],
    notes: 'A few touring goals',
    resolution: ''
  },
  {
    id: 'sample-decision-2',
    type: 'task',
    text: 'Book live music opportunity',
    parentGoalId: null,
    completed: false,
    scheduled: formatDate(DAYS_AHEAD[0]),
    scheduledEnd: '',
    hiddenUntil: null,
    tags: ['music'],
    notes: '',
    resolution: ''
  },
  {
    id: 'sample-decision-3',
    type: 'task',
    text: 'Follow up with press',
    parentGoalId: 'sample-decision-1',
    completed: false,
    scheduled: formatDate(DAYS_AHEAD[2]),
    scheduledEnd: '',
    hiddenUntil: null,
    tags: ['press'],
    notes: '',
    resolution: ''
  }
];

const rawLists = [
  {
    id: 'sample-list-1',
    name: 'Curated Reads',
    items: [
      {
        Title: 'https://example.com/deep-work',
        Title_label: 'Deep Work',
        Author: 'Cal Newport'
      },
      {
        Title: 'https://example.com/focus',
        Title_label: 'Flow State',
        Author: 'Mihaly Csikszentmihalyi'
      },
      {
        Title: 'https://example.com/habits',
        Title_label: 'Atomic Habits',
        Author: 'James Clear'
      }
    ]
  },
  {
    id: 'sample-list-2',
    name: 'Live Show Ideas',
    items: [
      {
        Title: 'https://example.com/venue',
        Title_label: 'Venue scouting',
        Author: 'Live Team'
      }
    ]
  }
];

const rawMetrics = [
  {
    id: 'metric-1',
    name: 'Focus Sessions',
    direction: 'higher',
    unit: 'sessions'
  }
];

const todayKey = new Date().toISOString().split('T')[0];
const rawMetricData = {
  [todayKey]: {
    'metric-1': [
      {
        timestamp: Date.now(),
        value: 1
      }
    ]
  }
};

export const SAMPLE_DECISIONS = deepFreeze(rawDecisions);
export const SAMPLE_LISTS = deepFreeze(rawLists);
export const SAMPLE_METRICS = deepFreeze(rawMetrics);
export const SAMPLE_METRIC_DATA = deepFreeze(rawMetricData);
