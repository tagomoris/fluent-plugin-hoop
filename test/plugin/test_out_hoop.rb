require 'helper'

class HoopOutputTest < Test::Unit::TestCase
  # setup/teardown and tests of dummy hoop server defined at the end of this class...

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
    
    assert_equal 'localhost:14000', d.instance.hoop_server
    assert_equal '/logs/from/fluentd/foo-%Y%m%d%H', d.instance.path
    assert_equal 'hoopuser', d.instance.username

    assert_nil d.instance.time_format
    assert_equal 'json', d.instance.output_type
    assert_equal false, d.instance.add_newline
    assert_equal 'TAB', d.instance.field_separator
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

  VALID_COOKIE_STRING = 'alfredo.auth="u=hoopuser&p=hoopuser&t=simple&e=1322203001386&s=SErpv88rOAVEItSOIoCtIV/DSpE="'
  RES_COOKIE_AUTH_FAILURE = WEBrick::Cookie.parse_set_cookie('alfredo.auth=""; Expires=Thu, 01-Jan-1970 00:00:10 GMT; Path=/')
  RES_COOKIE_AUTH_SUCCESS = WEBrick::Cookie.parse_set_cookie(VALID_COOKIE_STRING + '; Version=1; Path=/')
  RES_BODY_STATUS_ROOT = '{"path":"http:\/\/localhost:14000\/","isDir":true,"len":0,"owner":"hoopuser","group":"supergroup","permission":"-rwxr-xr-x","accessTime":0,"modificationTime":1320055230010,"blockSize":0,"replication":0}'
  RES_FORMAT_ALREADY_EXISTS = "{\"statusCode\":500,\"reason\":\"Internal Server Error\",\"message\":\"java.io.IOException: failed to create file %s on client 127.0.0.1 either because the filename is invalid or the file exists\",\"exception\":\"org.apache.hadoop.ipc.RemoteException\"}"
  RES_FORMAT_NOT_FOUND = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"java.io.FileNotFoundException: failed to append to non-existent file %s on client 127.0.0.1\",\"exception\":\"java.io.FileNotFoundException\"}"
  RES_FORMAT_NOT_FOUND_GET = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"File does not exist: %s\",\"exception\":\"java.io.FileNotFoundException\"}"

  CONTENT_TYPE_JSON = 'application/json'

  def setup
    Fluent::Test.setup
    @dummy_server_thread = Thread.new do
      srv = if ENV['FLUENT_TEST_DEBUG']
              logger = WEBrick::Log.new('/dev/null', WEBrick::BasicLog::DEBUG)
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000, :Logger => logger, :AccessLog => []})
            else
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000})
            end
      @fsdata = {}
      srv.mount_proc('/'){|req,res|
        # status only...
        if req.query['user.name'] or req.cookies.index{|item| item.name == 'alfredo.auth' and item.value}
          res.status = 200
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
          res.body = RES_BODY_STATUS_ROOT
        else
          res.cookies << RES_COOKIE_AUTH_FAILURE
          res.status = 401
        end
      }
      srv.mount_proc('/logs/from/fluentd') {|req, res|
        if req.request_method == 'POST' or req.request_method == 'PUT' or req.request_method == 'DELETE'
          # WEBrick's default handler ignores query parameter of URI without method GET
          req.query.update(Hash[*(req.request_line.split(' ')[1].split('?')[1].split('&').map{|kv|kv.split('=')}.flatten)])
        end
        case
        when (not req.query['user.name'] and req.cookies.index{|item| item.name == 'alfredo.auth' and item.value} < 0)
          res.cookies << RES_COOKIE_AUTH_FAILURE
          res.status = 401
        when (req.query['op'] == 'create' and @fsdata[req.path] and req.query['overwrite'] and req.query['overwrite'] == 'false')
          res.status = 500
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
          res.body = sprintf RES_FORMAT_ALREADY_EXISTS, req.path
        when req.query['op'] == 'create'
          @fsdata[req.path] = req.body
          res.status = 201
          res['Location'] = 'http://localhost:14000' + req.path
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
        when (req.query['op'] == 'append' and @fsdata[req.path])
          @fsdata[req.path] += req.body
          res.status = 200
          res['Location'] = 'http://localhost:14000' + req.path
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
        when req.query['op'] == 'append'
          res.status = 404
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
          res.body = sprintf RES_FORMAT_NOT_FOUND, req.path
        when (req.request_method == 'GET' and @fsdata[req.path]) # maybe GET
          res.status = 200
          res.content_type = 'application/octet-stream'
          res.cookies << RES_COOKIE_AUTH_SUCCESS
          res.body = @fsdata[req.path]
        else
          res.status = 404
          res.content_type = CONTENT_TYPE_JSON
          res.cookies << RES_COOKIE_AUTH_SUCCESS
          res.body = sprintf RES_FORMAT_NOT_FOUND_GET, req.path
        end
      }
      begin
        srv.start
      ensure
        srv.shutdown
      end
    end
    Thread.pass
  end

  def test_dummy_server
    d = create_driver
    authheader = {'Cookie' => VALID_COOKIE_STRING}
    client = Net::HTTP.start(d.instance.hoop_server.split(':')[0], d.instance.hoop_server.split(':')[1])
    assert_equal '401', client.request_get('/').code
    assert_equal '200', client.request_get('/?user.name=hoopuser').code
    assert_equal '200', client.request_get('/', authheader).code

    # /logs/from/fluentd
    path1 = '/logs/from/fluentd/hoge001/moge-access-log'
    path1_line1 = "1111111111111111111111111111111\n"
    path1_line2 = "2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path1 + '?op=append', path1_line1, authheader).code
    assert_equal '201', client.request_post(path1 + '?op=create&overwrite=false', path1_line1, authheader).code
    assert_equal path1_line1, client.request_get(path1, authheader).body
    assert_equal '200', client.request_put(path1 + '?op=append', path1_line2, authheader).code
    assert_equal path1_line1 + path1_line2, client.request_get(path1, authheader).body

    path2 = '/logs/from/fluentd/hoge002/moge-access-log'
    path2_line1 = "XXXXX___1111111111111111111111111111111\n"
    path2_line2 = "YYYYY___2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path2 + '?op=append', path2_line1, authheader).code
    assert_equal '201', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal '500', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal path2_line1, client.request_get(path2, authheader).body
    assert_equal '200', client.request_put(path2 + '?op=append', path2_line2, authheader).code
    assert_equal path2_line1 + path2_line2, client.request_get(path2, authheader).body
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end
end
