require 'helper'
# require 'time'
require 'webrick'

class HoopOutputTest < Test::Unit::TestCase
  # TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  def setup
    Fluent::Test.setup
    @dummy_server_thread = Thread.new do
      srv = WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000, :AccessLog => [[$log,'']]})
      srv.mount_proc('/logs/from/fluentd') {|req, res|
        # hogehoge
      }
      begin
        srv.start
      ensure
        srv.shutdown
      end
    end
    Thread.pass
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end

  # include Fluent::SetTagKeyMixin
  # config_set_default :include_tag_key, false

  # include Fluent::SetTimeKeyMixin
  # config_set_default :include_time_key, true

  # config_param :hoop_server, :string   # host:port
  # config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)
  # config_param :username, :string      # hoop pseudo username
  
  # config_param :time_format, :string, :default => nil
  # config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attributes:field1,field2,field3(...)'
  # config_param :add_newline, :bool,   :default => false
  # config_param :field_separator, :string, :default => 'TAB' # or SPACE,COMMA (for output_type=attributes:*)


  CONFIG = %[
    hoop_server localhost:14000
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::HoopOutput).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:14000
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:14000
    path /logs/from/fluentd/foo-%Y%m%d%H
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:xxx
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:xxx
    path logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }

    d = create_driver(CONFIG)
    
    assert_equal d.instance.hoop_server, 'hoop.master.local:14000'
    assert_equal d.instance.path, '/logs/from/fluentd/foo-%Y%m%d%H'
    assert_equal d.instance.username, 'hoopuser'

    assert_nil d.instance.time_format
    assert_equal d.instance.output_type, 'json'
    assert_equal d.instance.add_newline, false
    assert_equal d.instance.field_separator, 'TAB'
  end

  def test_format
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n]
    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    # d.run
  end

  def test_write
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # ### FileOutput#write returns path
    # path = d.run
    # expect_path = "#{TMP_DIR}/out_file_test._0.log.gz"
    # assert_equal expect_path, path

    # data = Zlib::GzipReader.open(expect_path) {|f| f.read }
    # assert_equal %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] +
    #                 %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n],
    #              data
  end
end
