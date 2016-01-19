/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include <deque>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/check.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/result.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "etcd/etcd.hpp"

using namespace process;

using std::string;
using std::deque;

namespace etcd {

Try<Node*> Node::parse(const JSON::Object& object)
{
  Owned<Node> node(new Node);

  Result<JSON::Number> createdIndex = object.find<JSON::Number>("createdIndex");

  if (createdIndex.isError()) {
    return Error("Failed to find 'createdIndex' in JSON: " +
                 createdIndex.error());
  } else if (createdIndex.isSome()) {
    node->createdIndex = createdIndex.get().value;
  }

  Result<JSON::String> expiration = object.find<JSON::String>("expiration");

  if (expiration.isError()) {
    return Error("Failed to find 'expiration' in JSON: " + expiration.error());
  } else if (expiration.isSome()) {
    node->expiration = expiration.get().value;
  }

  Result<JSON::String> key = object.find<JSON::String>("key");

  if (key.isError()) {
    return Error("Failed to find 'key' in JSON: " + key.error());
  } else if (key.isSome()) {
    node->key = key.get().value;
  } else if (key.isNone()) {
    return Error("Expecting 'key' in the JSON");
  }

  Result<JSON::Number> modifiedIndex =
    object.find<JSON::Number>("modifiedIndex");

  if (modifiedIndex.isError()) {
    return Error("Failed to find 'modifiedIndex' in JSON: " +
                 modifiedIndex.error());
  } else if (modifiedIndex.isSome()) {
    node->modifiedIndex = modifiedIndex.get().value;
  }

  Result<JSON::Number> ttl = object.find<JSON::Number>("ttl");

  if (ttl.isError()) {
    return Error("Failed to find 'ttl' in JSON: " + ttl.error());
  } else if (ttl.isSome()) {
    node->ttl = Seconds(ttl.get().value);
  }

  Result<JSON::String> value = object.find<JSON::String>("value");

  if (value.isError()) {
    return Error("Failed to find 'value' in JSON: " + value.error());
  } else if (value.isSome()) {
    node->value = value.get().value;
  }

  // TODO(benh): Parse 'dir' and 'nodes'.

  // TODO(benh): Do any necessary validation.

  return node.release();
}


Try<Response> Response::parse(const Try<JSON::Object>& object)
{
  if (object.isError()) {
    return Error(object.error());
  }

  Response response;

  // First check if this is an error.
  Result<JSON::Number> errorCode = object.get().find<JSON::Number>("errorCode");

  if (errorCode.isError()) {
    return Error("Failed to find 'errorCode' in JSON: " + errorCode.error());
  } else if (errorCode.isSome()) {
    response.errorCode = errorCode.get().value;

    Result<JSON::String> message = object.get().find<JSON::String>("message");

    if (message.isError()) {
      return Error("Failed to find 'message' in JSON" + message.error());
    } else if (message.isSome()) {
      response.message = message.get().value;
    }

    Result<JSON::String> cause = object.get().find<JSON::String>("cause");

    if (cause.isError()) {
      return Error("Failed to find 'cause' in JSON: " + cause.error());
    } else if (cause.isSome()) {
      response.cause = cause.get().value;
    }

    Result<JSON::Number> index = object.get().find<JSON::Number>("index");

    if (index.isError()) {
      return Error("Failed to find 'index' in JSON: " + index.error());
    } else if (index.isSome()) {
      response.index = index.get().value;
    }

    // TODO(benh): Do any necessary validation.

    // Not expecting anything else when the response is an error.
    return response;
  }

  Result<JSON::String> action = object.get().find<JSON::String>("action");

  if (action.isError()) {
    return Error("Failed to find 'action' in JSON: " + action.error());
  } else if (action.isSome()) {
    response.action = action.get().value;
  }

  // Check and see if we have a 'prevNode'.
  Node *previous = NULL;

  Result<JSON::Object> prevNode = object.get().find<JSON::Object>("prevNode");

  if (prevNode.isError()) {
    return Error("Failed to find 'prevNode' in JSON: " + prevNode.error());
  } else if (prevNode.isSome()) {
    Try<Node*> parse = Node::parse(prevNode.get());
    if (parse.isError()) {
      return Error("Failed to parse 'prevNode' in JSON: " + parse.error());
    }
    previous = parse.get();
  }

  Result<JSON::Object> node = object.get().find<JSON::Object>("node");

  if (node.isError()) {
    return Error("Failed to find 'node' in JSON: " + node.error());
  } else if (node.isSome()) {
    Try<Node*> parse = Node::parse(node.get());
    if (parse.isError()) {
      return Error("Failed to parse 'node' in JSON: " + parse.error());
    }
    Node *n = parse.get();
    n->previous.reset(previous);
    response.node = *n;
  }

  // Now validate the JSON.
  if (response.node.isNone()) {
    return Error("No 'errorCode', 'node', or 'prevNode' found");
  }

  return response;
}


// Helper for parsing an http::Response into an etcd::Response.
Future<etcd::Response> parse(const http::Response& response)
{
  if (response.type == http::Response::BODY) {
    Try<Response> parse =
      Response::parse(JSON::parse<JSON::Object>(response.body));

    if (parse.isError()) {
      return Failure("Failed to parse response from etcd: " + parse.error());
    }

    return parse.get();
  }

  return Failure("Expecting body in response");
}


// Helper for creating a Failure from an etcd::Response.
Failure failure(const Response& response)
{
  CHECK_SOME(response.errorCode);

  string message =
    "etcd returned error code " + stringify(response.errorCode.get());

  if (response.message.isSome()) {
    message += ": " + response.message.get();
  }

  if (response.cause.isSome()) {
    message += " (" + response.cause.get() + ")";
  }

  return Failure(message);
}


// Forward declarations of continuations.
static Future<http::Response> _create(deque<http::URL> urls);
static Future<Option<Node>> __create(const Response& response);

static Future<http::Response> _get(deque<http::URL> urls);
static Future<Option<Node>> __get(const Response& response);

static Future<http::Response> _watch(deque<http::URL> urls);
static Future<Option<Node>> __watch(const Response& response);


Future<Option<Node>> create(
    const URL& _url,
    const string& value,
    const Option<Duration>& ttl,
    const Option<bool> prevExist,
    const Option<uint64_t>& prevIndex,
    const Option<string>& prevValue)
{
  // Transform the etcd URL into a collection of HTTP URLs.
  deque<http::URL> urls;

  foreach (const URL::Server& server, _url.servers) {
    // TODO(benh): Use HTTPS after supported in libprocess.
    http::URL url("http", server.host, server.port, _url.path);

    url.query["value"] = value;

    if (ttl.isSome()) {
      // Because etcd expects TTLs as integer seconds we need cast the
      // double we get back from Duration::secs() to an integer before
      // we turn it into a string.
      url.query["ttl"] = stringify(uint64_t(ttl.get().secs()));
    }

    if (prevExist.isSome()) {
      url.query["prevExist"] = stringify(prevExist.get());
    }

    if (prevIndex.isSome()) {
      url.query["prevIndex"] = stringify(prevIndex.get());
    }

    if (prevValue.isSome()) {
      url.query["prevValue"] = stringify(prevValue.get());
    }

    urls.push_back(url);
  }

  // TODO(benh): Randomize ordering of URLs or some how create a
  // structure to know which one was used in the past and use that
  // one. The latter would be easier if we actually had an 'Etcd'
  // object from which we made 'create', 'get', 'watch', etc, not take
  // the entire etcd::URL but instead just took the necessary
  // parameters (like, 'key', 'value', etc).

  return _create(urls)
    .then(lambda::bind(&parse, lambda::_1))
    .then(lambda::bind(&__create, lambda::_1));
}


static Future<http::Response> _create(deque<http::URL> urls)
{
  if (urls.empty()) {
    return Failure("Exhaustively tried all etcd servers; giving up");
  }

  http::URL url = urls.front();

  urls.pop_front();

  // TODO(benh): Add connection timeout once supported by http::put.
  return http::put(url)
    .repair(lambda::bind(&_create, urls));
}


static Future<Option<Node>> __create(const Response& response)
{
  if (response.errorCode.isSome()) {
    // If the key already exists, or had the wrong value we return
    // None rather than error.
    // 101 means "Compare failed", 105 means "Key already exists"
    if (response.errorCode.get() == 101 || response.errorCode.get() == 105) {
      return None();
    }
    return failure(response);
  } else if (response.node.isNone()) {
    return Failure("Expecting 'node' in response");
  };
  // Previous might be some because we aren't always strictly
  // creation, just preconditioned.
  return response.node.get();
}


Future<Option<Node>> get(const URL& _url)
{
  // Transform the etcd URL into a deque of HTTP URLs.
  deque<http::URL> urls;

  foreach (const URL::Server& server, _url.servers) {
    // TODO(benh): Use HTTPS after supported in libprocess.
    http::URL url("http", server.host, server.port, _url.path);

    url.query["quorum"] = "true";

    urls.push_back(url);
  }

  // TODO(benh): See TODO in 'create' for randomizing ordering of URLs.

  return _get(urls)
    .then(lambda::bind(&parse, lambda::_1))
    .then(lambda::bind(&__get, lambda::_1));
}


static Future<http::Response> _get(deque<http::URL> urls)
{
  if (urls.empty()) {
    return Failure("Exhaustively tried all etcd servers; giving up");
  }

  http::URL url = urls.front();

  urls.pop_front();

  // TODO(benh): Add connection timeout once supported by http::get.
  return http::get(url)
    .repair(lambda::bind(&_get, urls));
}


static Future<Option<Node>> __get(const Response& response)
{
  // If this key is just missing then return none, otherwise return a
  // Failure and attempt to provide the 'message'.
  if (response.errorCode.isSome()) {
    if (response.errorCode.get() == 100) {
      return None();
    }
    return failure(response);
  } else if (response.node.isNone()) {
    return Failure("Expecting 'node' in response");
  } else if (response.node.get().previous.get()) {
    return Failure("Not expecting 'prevNode' in response");
  }

  return response.node.get();
}


Future<Option<Node>> watch(
    const URL& _url,
    const Option<uint64_t>& waitIndex)
{
  // Transform the etcd URL into a deque of HTTP URLs.
  deque<http::URL> urls;

  foreach (const URL::Server& server, _url.servers) {
    // TODO(benh): Use HTTPS after supported in libprocess.
    http::URL url("http", server.host, server.port, _url.path);

    url.query["wait"] = "true";

    if (waitIndex.isSome()) {
      url.query["waitIndex"] = stringify(waitIndex.get());
    }

    urls.push_back(url);
  }

  // TODO(benh): See TODO in 'create' for randomizing ordering of URLs.

  return _watch(urls)
    .then(lambda::bind(&parse, lambda::_1))
    .then(lambda::bind(&__watch, lambda::_1));
}


static Future<http::Response> _watch(deque<http::URL> urls)
{
  if (urls.empty()) {
    return Failure("Exhaustively tried all etcd servers; giving up");
  }

  http::URL url = urls.front();

  urls.pop_front();

  // TODO(benh): Add connection timeout once supported by http::get.
  return http::get(url)
    .repair(lambda::bind(&_watch, urls));
}


static Future<Option<Node>> __watch(const Response& response)
{
  if (response.errorCode.isSome()) {
    return failure(response);
  } else if (response.action.isSome()) {
    // If the key has been deleted then return None.
    if (response.action.get() == "delete" ||
        response.action.get() == "compareAndDelete") {
      return None();
    }

    // Return the node if it exists and the action didn't delete.
    if (response.node.isSome()) {
      return response.node.get();
    }
  }

  return Failure("Expecting 'action' in response");
}

} // namespace etcd {
