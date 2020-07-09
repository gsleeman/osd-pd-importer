#!/usr/bin/python3

# fetches OSD hive service pagerduty incidents+alerts and writes to alerts.json
#
# requires pdpyras and yaml
#
# expects pagerduty api key to be placed in /run/secrets/pagerduty/PAGERDUTY_KEY
# (can override api key path with PD_API_KEY_PATH)

import json, yaml, os
from pdpyras import APISession
from pathlib import Path
from datetime import datetime, timedelta

api_token = Path(os.getenv('PD_API_KEY_PATH', '/run/secrets/pagerduty/PAGERDUTY_KEY')).read_text()
policy_id = os.getenv('POLICY_ID', 'PA4586M')
team_id = os.getenv('TEAM_ID', 'PASPK4G')

alerts_db = 'alerts.json'

pd = APISession(api_token)
pd.rget('escalation_policies/' + policy_id)

try:
    db = json.loads(Path(alerts_db).read_text())
    db.sort(key=lambda i: datetime.strptime(i['created_at']+'+00:00', '%Y-%m-%dT%H:%M:%SZ%z'))
    since = db[-1]['created_at']
except:
    db, since = [], datetime.utcnow() - timedelta(days=90)

seen = [alert['id'] for alert in db]

def status_reporter(db):
    def print_status(_, num, total):
        if num % 100 == 1:
            print("Processed %d/%d incidents" % (num - 1, total))
            Path(alerts_db).write_text(json.dumps(db))
    return print_status

for incident in pd.iter_all('incidents', total=True, item_hook=status_reporter(db),
                             params={'since': since, 'until': datetime.utcnow(),
                                     'time_zone': 'UTC', 'sort_by': 'created_at:ASC',
                                     'team_ids[]': [team_id], 'statuses[]': ['resolved'],
                                     'include[]': ['users','assignees','services','acknowledgers',
                                                   'assignments','acknowledgements']}):
    if 'name' not in incident['service'].keys(): continue
    if not incident['service']['name'].endswith('-hive-cluster'): continue
    incident['log_entries'] = [
        entry for entry in pd.rget('/incidents/%s/log_entries' % incident["id"], params={
            'sort_by': 'created_at:asc', 'is_overview': 'true', 'time_zone': 'UTC'})
        if True or entry['log_entry_type'].startswith(('resolve', 'assign', 'acknowledge'))]
    for alert in pd.rget('incidents/%s/alerts' % incident["id"], params={'sort_by': 'created_at:asc'}):
        if alert['id'] in seen:
            continue
        metadata = yaml.full_load(alert['body']['details']['firing'])
        for key in 'Labels', 'Annotations':
            if key in metadata.keys() and type(metadata[key]) is list:
                metadata[key.lower()] = {k.strip(): v.strip()
                    for k,v in [kv.partition("=")[::2] for kv in metadata.pop(key) if '=' in kv]}
        alert['metadata'] = metadata
        alert['incident'] = incident
        db.append(alert)

print("%d new alerts(s) imported. %d alerts in database" % (len(db) - len(seen), len(db)))

Path(alerts_db).write_text(json.dumps(db))

