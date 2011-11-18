class Fluent::HoopOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('hoop', self)

  include Fluent::SetTagKeyMixin
  config_set_default :include_tag_key, false

  include Fluent::SetTimeKeyMixin
  config_set_default :include_time_key, true

  config_param :hoop_server, :string   # host:port
  config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag)
  config_param :username, :string      # hoop pseudo username
  
  config_param :time_format, :string, :default => nil
  config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attributes:field1,field2,field3(...)'
  config_param :add_newline, :bool,   :default => true
  config_param :field_separator, :string, :default => 'TAB' # or SPACE,COMMA (for output_type=attributes:*)

  def initialize
    super
    require 'net/http'
  end

  def configure(conf)
    super
    # @path = conf['path']
  end

  def start
    super
    # init
  end

  def shutdown
    super
    # destroy
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    records = []
    chunk.msgpack_each { |record|
      # records << record
    }
    # write records
  end
end
