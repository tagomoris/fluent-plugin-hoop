class Fluent::HoopOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('hoop', self)

  config_set_default :buffer_type, 'memory'

  include Fluent::SetTagKeyMixin
  config_set_default :include_tag_key, false

  include Fluent::SetTimeKeyMixin
  config_set_default :include_time_key, true

  config_param :hoop_server, :string   # host:port
  config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)
  config_param :username, :string      # hoop pseudo username
  
  config_param :time_format, :string, :default => nil
  config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attributes:field1,field2,field3(...)'
  config_param :add_newline, :bool,   :default => false
  config_param :field_separator, :string, :default => 'TAB' # or SPACE,COMMA (for output_type=attributes:*)
  # config_param :tag_format, :string, :default => 'all' # or 'last'(last.part.of.tag => tag) or 'none'

  def initialize
    super
    require 'net/http'
  end

  def configure(conf)
    super
    unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ conf['hoop_server']
      raise Fluent::ConfigError, "Invalid config value on hoop_server: '#{conf['hoop_server']}', needs SERVER_NAME:PORT"
    end
    @host = $1
    @port = $2.to_i
    @path = conf['path']
    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{conf['path']}'"
    end
    @username = conf['username']
    @conn = nil
    @header = {'Content-Type' => 'application/octet-stream'}
  end

  def start
    super
    @conn = Net::HTTP.start(@host, @port)
    $log.info "connected hoop server: #{@host} port #{@port}"
  end

  def shutdown
    super
    @conn.finish
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
