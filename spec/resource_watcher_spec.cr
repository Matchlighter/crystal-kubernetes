require "./spec_helper"

require "../src/kubernetes"

private struct WatchSpecResource
  include JSON::Serializable

  getter metadata : Kubernetes::Metadata
end

private class StopWatchSpec < Exception
end

private def watch_event(type : String, name : String, resource_version : String)
  {
    type:   type,
    object: {
      kind:       "WatchSpecResource",
      apiVersion: "example.test/v1",
      metadata:   {
        name:            name,
        resourceVersion: resource_version,
      },
    },
  }.to_json
end

private def receive_with_timeout(channel : Channel(T)) : T forall T
  select
  when value = channel.receive
    value
  when timeout(5.seconds)
    raise "Timed out waiting for watch event"
  end
end

describe Kubernetes::ResourceWatcher do
  it "delivers events through Client#watch_resource" do
    server = HTTP::Server.new do |context|
      context.response.content_type = "application/json"
      context.response.puts watch_event("ADDED", "callback-item", "1")
    end
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    client = Kubernetes::Client.new(
      server: URI.parse("http://#{address}"),
      token: -> { "" },
      tls: nil,
    )
    received_name = nil

    expect_raises(StopWatchSpec) do
      client.watch_resource(WatchSpecResource, "/items") do |watch|
        received_name = watch.object.metadata.name
        raise StopWatchSpec.new
      end
    end
    received_name.should eq("callback-item")
  ensure
    client.try(&.close)
    server.try(&.close)
  end

  it "reconnects after the server ends a watch response" do
    request_count = Atomic(Int32).new(0)
    server = HTTP::Server.new do |context|
      request_number = request_count.add(1)
      context.response.content_type = "application/json"
      context.response.puts watch_event("ADDED", "item-#{request_number}", request_number.to_s)
    end
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    client = Kubernetes::Client.new(
      server: URI.parse("http://#{address}"),
      token: -> { "" },
      tls: nil,
    )
    watcher = Kubernetes::ResourceWatcher(WatchSpecResource).new(client, "/items")
    events = Channel(String).new
    watcher.on_change { |watch| events.send(watch.object.metadata.name) }

    spawn watcher.start_watching!

    receive_with_timeout(events).should eq("item-0")
    receive_with_timeout(events).should eq("item-1")
    request_count.get.should be >= 2
  ensure
    watcher.try(&.close)
    client.try(&.close)
    server.try(&.close)
  end

  it "clears and rebuilds a synced store after resource-version expiry" do
    requested_versions = Channel(String).new(3)
    request_count = Atomic(Int32).new(0)
    server = HTTP::Server.new do |context|
      requested_versions.send(context.request.query_params["resourceVersion"]? || "")
      request_number = request_count.add(1)
      context.response.content_type = "application/json"

      case request_number
      when 0
        context.response.puts watch_event("ADDED", "stale", "7")
      when 1
        context.response.status = :gone
        context.response.puts({
          kind:       "Status",
          apiVersion: "v1",
          metadata:   {} of String => String,
          status:     "Failure",
          message:    "too old resource version",
          reason:     "Expired",
          code:       410,
        }.to_json)
      else
        context.response.puts watch_event("ADDED", "fresh", "9")
      end
    end
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    client = Kubernetes::Client.new(
      server: URI.parse("http://#{address}"),
      token: -> { "" },
      tls: nil,
    )
    watcher = Kubernetes::ResourceWatcher(WatchSpecResource).new(client, "/items")
    store = Kubernetes::SyncedStore(WatchSpecResource).new(watcher)
    changes = Channel(String).new
    store.on_change { |watch| changes.send(watch.object.metadata.name) }
    store.spawn_watch

    receive_with_timeout(changes).should eq("stale")
    receive_with_timeout(changes).should eq("fresh")

    store.all.map(&.metadata.name).should eq(["fresh"])
    receive_with_timeout(requested_versions).should eq("0")
    receive_with_timeout(requested_versions).should eq("7")
    receive_with_timeout(requested_versions).should eq("")
  ensure
    store.try(&.disconnect)
    client.try(&.close)
    server.try(&.close)
  end
end
