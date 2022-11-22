import apache_beam as beam

with beam.Pipeline() as pipeline:
    icon_pairs = pipeline | 'Create icons' >> beam.Create(
        [
            ('Apple', '🍎'),
            ('Apple', '🍏'),
            ('Eggplant', '🍆'),
            ('Tomato', '🍅'),
        ]
    )

    duration_pairs = pipeline | 'Create durations' >> beam.Create(
        [
            ('Apple', 'perennial'),
            ('Carrot', 'biennial'),
            ('Tomato', 'perennial'),
            ('Tomato', 'annual'),
        ]
        | beam.Map(print)
    )

    # plants = (({
    #     'icons': icon_pairs, 'durations': duration_pairs
    # })
    #           | 'Merge' >> beam.CoGroupByKey()
    #           | beam.Map(print))
