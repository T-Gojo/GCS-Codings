import apache_beam as beam

TOPIC_PATH = "projects/practice-id1/topics/input2"


def CombineFn(e):
    return "\n".join(e)


o = beam.options.pipeline_options.PipelineOptions()
p = beam.Pipeline(options=o)
data = (p | "Read From Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_PATH)
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(30))
        | "Combine" >> beam.transforms.core.CombineGlobally(CombineFn).without_defaults()
        | "Output" >> beam.io.WriteToText(r"C:\Users\prasanna_parida\PycharmProjects\gcsDemos\DataflowPipeline\resource\output"))

res = p.run()
res.wait_until_finish()
