import apache_beam as beam

pc = [1, 10, 100, 1000]


def bounded_sum(values, bound=500):
    return min(sum(values), bound)


small_sum = (pc | beam.CombineGlobally(bounded_sum)
             | beam.Map(print)
             )# [500]
large_sum = pc | beam.CombineGlobally(bounded_sum, bound=5000)  # [1111]
