Embulk::JavaPlugin.register_output(
  "marketo_leadbulk", "org.embulk.output.marketo_leadbulk.MarketoLeadbulkOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
