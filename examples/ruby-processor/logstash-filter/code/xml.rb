# java_import `org.opensearch.dataprepper.logging.DataPrepperMarkers`
require 'json'
require 'jmespath'
require "xmlsimple"
require 'nokogiri'

def init(params)
  $params = params.clone
  puts $params
  @xml_filter = Xml.new
  @xml_filter.register
end


def process(event)
  puts @xml_filter
  @xml_filter.filter(event)
end

# encoding: utf-8



# XML filter. Takes a field that contains XML and expands it into
# an actual datastructure.
class Xml

  # Config for xml to hash is:
  # [source,ruby]
  #     source => source_field
  #
  # For example, if you have the whole XML document in your `message` field:
  # [source,ruby]
  #     filter {
  #       xml {
  #         source => "message"
  #       }
  #     }
  #
  # The above would parse the XML from the `message` field.
  # config :source, :validate => :string, :required => true

  # Define target for placing the data
  #
  # For example if you want the data to be put in the `doc` field:
  # [source,ruby]
  #     filter {
  #       xml {
  #         target => "doc"
  #       }
  #     }
  #
  # XML in the value of the source field will be expanded into a
  # datastructure in the `target` field.
  # Note: if the `target` field already exists, it will be overridden.
  # Required if `store_xml` is true (which is the default).
  # config :target, :validate => :string

  # xpath will additionally select string values (non-strings will be
  # converted to strings with Ruby's `to_s` function) from parsed XML
  # (using each source field defined using the method above) and place
  # those values in the destination fields. Configuration:
  # [source,ruby]
  # xpath => [ "xpath-syntax", "destination-field" ]
  #
  # Values returned by XPath parsing from `xpath-syntax` will be put in the
  # destination field. Multiple values returned will be pushed onto the
  # destination field as an array. As such, multiple matches across
  # multiple source fields will produce duplicate entries in the field.
  #
  # More on XPath: https://www.w3schools.com/xml/xml_xpath.asp
  #
  # The XPath functions are particularly powerful:
  # https://www.w3schools.com/xml/xsl_functions.asp
  #
  # config :xpath, :validate => :hash, :default => {}

  # Supported XML parsing options are 'strict', 'no_error' and 'no_warning'.
  # - strict mode turns on strict parsing rules (non-compliant xml will fail)
  # - no_error and no_warning can be used to suppress errors/warnings
  # config :parse_options, :validate => :string
  # NOTE: technically we support more but we purposefully do not document those.
  # e.g. setting "strict|recover" will not turn on strict as they're conflicting

  # By default the filter will store the whole parsed XML in the destination
  # field as described above. Setting this to false will prevent that.
  # config :store_xml, :validate => :boolean, :default => true

  # By default the filter will force single elements to be arrays. Setting this to
  # false will prevent storing single elements in arrays.
  # config :force_array, :validate => :boolean, :default => true

  # By default the filter will expand attributes differently from content inside
  # of tags. This option allows you to force text content and attributes to always
  # parse to a hash value.
  # config :force_content, :validate => :boolean, :default => false

  # By default only namespaces declarations on the root element are considered.
  # This allows to configure all namespace declarations to parse the XML document.
  #
  # Example:
  #
  # [source,ruby]
  # filter {
  #   xml {
  #     namespaces => {
  #       "xsl" => "http://www.w3.org/1999/XSL/Transform"
  #       "xhtml" => http://www.w3.org/1999/xhtml"
  #     }
  #   }
  # }
  #
  # config :namespaces, :validate => :hash, :default => {}

  # Remove all namespaces from all nodes in the document.
  # Of course, if the document had nodes with the same names but different namespaces, they will now be ambiguous.
  # config :remove_namespaces, :validate => :boolean, :default => false

  # By default, output nothing if the element is empty.
  # If set to `false`, empty element will result in an empty hash object.
  # config :suppress_empty, :validate => :boolean, :default => true

  XMLPARSEFAILURE_TAG = "_xmlparsefailure"

  def register
    # require "xmlsimple"
    puts "in register method..."
    puts $params
    @source = $params.key?('source') ? $params['source'] : nil
    @target = $params.key?('target') ? $params['target'] : nil
    @xpath = $params.key?('xpath') ? JSON.parse($params['xpath'].gsub("'", '"')) : {}
    @parse_options = $params.key?('parse_options') ? $params['parse_options'] : nil
    @store_xml = $params.key?('store_xml') ? $params['store_xml'].downcase == 'true' : true
    @force_array = $params.key?('force_array') ? $params['force_array'].downcase == 'true' : true
    @force_content = $params.key?('force_content') ? $params['force_content'].downcase == 'true' : false
    @namespaces = $params.key?('namespaces') ? JSON.parse($params['namespaces'].gsub("'", '"')) : {}
    @remove_namespaces = $params.key?('remove_namespaces') ? $params['remove_namespaces'].downcase == 'true' : false
    @suppress_empty = $params.key?('suppress_empty') ? $params['suppress_empty'].downcase == 'true' : true


    if @store_xml && (!@target || @target.empty?)
      LOG.error("When the 'store_xml' configuration option is true, 'target' must also be set")
      raise Error
      # raise LogStash::ConfigurationError, I18n.t(
      #   "logstash.runner.configuration.invalid_plugin_register",
      #   :plugin => "filter",
      #   :type => "xml",
      #   :error => "When the 'store_xml' configuration option is true, 'target' must also be set"
      # )
    end
    xml_parse_options # validates parse_options => ...
  end

  def filter(event)
    matched = false

    # @logger.debug? && @logger.debug("Running xml filter", :event => event)

    value = event.get(@source)
    return unless value

    if value.is_a?(Array)
      if value.length != 1
        event.tag(XMLPARSEFAILURE_TAG)
        # @logger.warn("XML filter expects single item array", :source => @source, :value => value)
        LOG.error(
          # DataPrepperMarkers.SENSITIVE,
                  "XML filter expects single item array")
        return
      end

      value = value.first
    end

    unless value.is_a?(String)
      # event.tag(XMLPARSEFAILURE_TAG)
      event.getMetadata().addTags([XMLPARSEFAILURE_TAG])
      # @logger.warn("XML filter expects a string but received a #{value.class}", :source => @source, :value => value)
      LOG.error(
        # DataPrepperMarkers.SENSITIVE,
                "XML filter expects a string but received a #{value.class}", value)

      return
    end

    # Do nothing with an empty string.
    return if value.strip.empty?

    if @xpath
      begin
        doc = Nokogiri::XML::Document.parse(value, nil, value.encoding.to_s, xml_parse_options)
      rescue => e
        # event.tag(XMLPARSEFAILURE_TAG)
        event.getMetadata().addTags([XMLPARSEFAILURE_TAG])
        LOG.error(
          # DataPrepperMarkers.SENSITIVE,
          "Error parsing xml", value, e)

        # @logger.warn("Error parsing xml", :source => @source, :value => value, :exception => e, :backtrace => e.backtrace)
        return
      else
        LOG.debug("Parsed xml with #{doc.errors.size} errors")
        # doc.errors.any? && @logger.debug? && @logger.debug("Parsed xml with #{doc.errors.size} errors")
      end
      doc.remove_namespaces! if @remove_namespaces

      @xpath.each do |xpath_src, xpath_dest|
        nodeset = @namespaces.empty? ? doc.xpath(xpath_src) : doc.xpath(xpath_src, @namespaces)

        # If asking xpath for a String, like "name(/*)", we get back a
        # String instead of a NodeSet.  We normalize that here.
        normalized_nodeset = nodeset.kind_of?(Nokogiri::XML::NodeSet) ? nodeset : [nodeset]

        # Initialize empty resultset
        data = []

        normalized_nodeset.each do |value|
          # some XPath functions return empty arrays as string
          next if value.is_a?(Array) && value.length == 0

          if value
            matched = true
            data << value.to_s
          end

        end
        # set the destination attribute, if it's an array with a bigger size than one, leave as is. otherwise make it a string. added force_array param to provide same functionality as writing it in an xml target
        if data.size == 1 && !@force_array
          event.put(xpath_dest, data[0])
        else
          event.put(xpath_dest, data) unless data.nil? || data.empty?
        end
      end
    end

    if @store_xml
      begin
        xml_options = {"ForceArray" => @force_array, "ForceContent" => @force_content}
        xml_options["SuppressEmpty"] = true if @suppress_empty
        event.put(@target, XmlSimple.xml_in(value, xml_options))
        # event.put(@target, nokogiri_in(value, xml_options))

        matched = true
      rescue => e
        # event.tag(XMLPARSEFAILURE_TAG)
        event.getMetadata().addTags([XMLPARSEFAILURE_TAG])
        #
        LOG.error(
          # DataPrepperMarkers.SENSITIVE,
                  "Error parsing xml with XmlSimple: ", value, e)
        # @logger.warn("Error parsing xml with XmlSimple", :source => @source, :value => value, :exception => e, :backtrace => e.backtrace)
        return
      end
    end

    # filter_matched(event) if matched
    # @logger.debug? && @logger.debug("Event after xml filter", :event => event)
  rescue => e
    # event.tag(XMLPARSEFAILURE_TAG)
    event.getMetadata().addTags([XMLPARSEFAILURE_TAG])

    log_payload = { :exception => e.message, :source => @source }
    log_payload[:value] = value unless value.nil?
    log_payload[:backtrace] = e.backtrace if @logger.debug?
    LOG.error(
      # DataPrepperMarkers.SENSITIVE,
              "XML Parse Error", log_payload)
    # @logger.warn("XML Parse Error", log_payload)
  end

  private

  def xml_parse_options
    return Nokogiri::XML::ParseOptions::DEFAULT_XML unless @parse_options # (RECOVER | NONET)
    @xml_parse_options ||= begin
      parse_options = @parse_options.split(/,|\|/).map do |opt|
        name = opt.strip.tr('_', '').upcase
        if name.empty?
          nil
        else
          begin
            Nokogiri::XML::ParseOptions.const_get(name)
          rescue NameError
            raise LogStash::ConfigurationError, "unsupported parse option: #{opt.inspect}"
          end
        end
      end
      parse_options.compact.inject(0, :|) # e.g. NOERROR | NOWARNING
    end
  end
end
