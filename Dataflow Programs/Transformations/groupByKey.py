import apache_beam as beam

records = [
    ("sai", [27, "engineer"]),
    ("neetu", [27, "developer"]),
    ("faraq", [21, "engineer"]),
    ("sai", 'Unemployed'),
    ("neetu", "Empoyed"),
    ("kabita", [21, "Data Engineer"]),
    ('Kabita', "Employed"),
    ("faraq", [22, "web developer"])

]
with beam.Pipeline() as pipe:
    ip = (
            pipe
            | beam.Create(records)
            | beam.GroupByKey()
            | beam.Map(print)
    )
