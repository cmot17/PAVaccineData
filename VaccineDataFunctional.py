from os import read
import requests
import json
from datetime import datetime
import csv
import functools as ft

url = 'https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true'
myobj = """{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"c","Entity":"Counts of Vaccinations by County of Residence","Type":0},{"Name":"n","Entity":"Navigation","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"date"},"Name":"covid-immunizations-county.date"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"partial"}},"Function":0},"Name":"Sum(covid-immunizations-county.partial)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"full"}},"Function":0},"Name":"Sum(covid-immunizations-county.full)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"county_name"}}],"Values":[[{"Literal":{"Value":"'Allegheny'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Page name"}}],"Values":[[{"Literal":{"Value":"'Demographics'"}}]]}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,2,1],"ShowItemsWithNoData":[0]}]},"DataReduction":{"DataVolume":4,"Primary":{"Sample":{}}},"Version":1}}}]},"QueryId":"","ApplicationContext":{"DatasetId":"1e549164-aeab-4dcf-a97f-f70de27e715d","Sources":[{"ReportId":"53c72848-c634-4597-a571-54c087a01780"}]}}],"cancelQueries":[],"modelId":314528}"""
#This function will attempt to retreive an element from an array, and if it does not exist will return None
def try_get(x):
  def try_get_fn(i):
    try:
      return x[i]
    except IndexError:
      return None
    except KeyError:
      return None
  return try_get_fn

def reshuffleData(element, lastElement):
  [time, d1, d2, bitmask] = element
  [_, lastD1, lastD2, _] = lastElement #"_" ignores value
  if (bitmask == 0):
    dValues = d1, d2
  if (bitmask == 2):
    dValues = lastD1, d1
  if (bitmask == 4):
    dValues = d1, lastD1
  if (bitmask == 6):
    dValues = lastD1, lastD2
  return [time, *dValues]

x = requests.post(url, data = myobj)

#print(x)
#print(x.content)

resultRaw = json.loads(x.content.decode("utf-8"))
result = resultRaw["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0:]

#print(result)

dataWithNulls = list(map(
    lambda x: list( map( try_get(x["C"]), range(3) ) ) + [ try_get(x)("R") or 0 ],
    result
))

#print(list(dataWithNulls))

def dbg(x):
  print(x)
  return x

fixedData = ft.reduce(
  lambda a, x: ([*a[0], reshuffleData(x, [*a[1], None])], reshuffleData(x, [*a[1], None])),
  dataWithNulls,
  ([], dataWithNulls[0][:3])
)[0]

#print(list(fixedData))

readableDates = map(
    lambda x: [datetime.utcfromtimestamp(x[0] / 1000).strftime('%Y-%m-%d'), x[1], x[2]],
    fixedData
)

#print(list(readableDates))

with open('VaccineData.csv', 'w', newline='') as f:
    writer = csv.writer(f, delimiter=',', escapechar='\\', quoting=csv.QUOTE_NONE)
    for i in readableDates:
        writer.writerow(i)