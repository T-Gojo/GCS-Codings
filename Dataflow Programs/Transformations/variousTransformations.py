import apache_beam as beam

side_list = list()
with open('resource\exclude_ids.txt', 'r') as my_file:
    for line in my_file:
        side_list.append(line.rstrip())

p = beam.Pipeline()


class RemovingBrandNames(beam.DoFn):
    def process(self, element, side_list):
        brandName = element.split(',')[1]
        element_list = element.split(',')
        if brandName not in side_list:
            return [element_list]


initial_pc = (
        p
        | "Read from text file" >> beam.io.ReadFromText('resource\myntraDatasheet.csv', skip_header_lines=True)
        | "ParDo with side inputs" >> beam.ParDo(RemovingBrandNames(), side_list)
    # | beam.Filter(lambda record: record[4] == 'Men')

)
dressWear_pc = (
        initial_pc
        | beam.Map(lambda record: (record[1], record[3]))
        | beam.GroupByKey()
        | beam.Map(lambda record: (record[0], list(set(record[1]))))
    # | beam.Map(print)
)
dressWearCategories_pc = (
        initial_pc
        | beam.Map(lambda record: (record[1], record[2]))
        | "category Grouped" >> beam.GroupByKey()
        | beam.Map(lambda record: (record[0], list(set(record[1]))))
    # | beam.Map(print)
)

# remarksData = [
#     ('LOCOMOTIVE', 'good collections'),
#     ('HIGHLANDER', 'excellent collections'),
#     ('HERE&NOW', 'nice collections'),
#     ('HRX by Hrithik Roshan', 'good'),
#     ('WROGN', 'Excellent'),
#     ('Moda Rapido', 'Need improvement'),
#     ('Levis', 'increase more'),
#     ('Jockey', 'increase more'),
#     ('Mast & Harbour', 'good collections')
# ]
# remarks_pc = (
#     p | beam.Create(remarksData)
# )
# women_counts = (
#         p
#         | beam.io.ReadFromText('resource\myntraDatasheet.csv', skip_header_lines=True)
#         | beam.ParDo(RemovingBrandNames(), side_list)
#         | beam.Filter(lambda record: record[4] == 'Women')
#         | "lambda women" >> beam.Map(lambda record: (record[1], 1))
#         | "women" >> beam.CombinePerKey(sum)
#         #| "women print" >> beam.Map(print)
# )


#
# Flatten takes a tuple of PCollection objects.
# Returns a single PCollection that contains all the elements in the PCollection objects in that tuple.
# merged = (
#         (dressWearCategories_pc, dressWear_pc)
#         # A list of tuples can be "piped" directly into a Flatten transform.
#         | beam.Flatten()
#     # | beam.Map(print)
# )

combine_both = (
        ({'Category': dressWearCategories_pc, 'Individual': dressWear_pc})
        | "Merge" >> beam.CoGroupByKey()
        | "print Merge" >> beam.Map(print)
    )

p.run()
