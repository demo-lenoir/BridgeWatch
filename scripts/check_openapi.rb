require 'yaml'

document = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: false)
abort 'OpenAPI version mismatch' unless document['openapi'] == '3.0.3'
required = %w[/v1/messages/{id} /v1/messages /v1/status /v1/anomalies /health/live /health/ready /metrics]
abort 'missing API path' unless (required - document.fetch('paths').keys).empty?

walk = lambda do |node|
  case node
  when Hash
    if node.key?('$ref')
      ref = node.fetch('$ref')
      abort "external reference: #{ref}" unless ref.start_with?('#/')
      target = ref.delete_prefix('#/').split('/').reduce(document) { |value, part| value.fetch(part.gsub('~1', '/').gsub('~0', '~')) }
      abort "empty reference: #{ref}" if target.nil?
    end
    node.each_value { |value| walk.call(value) }
  when Array
    node.each { |value| walk.call(value) }
  end
end
walk.call(document)

limit = document.dig('components', 'parameters', 'Limit', 'schema')
abort 'unbounded collection' unless limit['maximum'] == 100 && limit['minimum'] == 1
puts 'OpenAPI YAML, paths, references, and collection bound: OK'
