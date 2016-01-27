// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>

#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/fetcher_plugin.hpp>

#include <mesos/uri/fetcher.hpp>

#include <stout/try.hpp>

#include "uri/fetchers/curl.hpp"

using namespace mesos;

using process::Owned;

using mesos::uri::CurlFetcherPlugin;

using mesos::uri::Fetcher;


// Declares a test FetcherPlugin module named
// 'org_apache_mesos_TestCurlFetcherPlugin'.
mesos::modules::Module<Fetcher::Plugin>
org_apache_mesos_TestCurlFetcherPlugin(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Curl Fetcher Plugin module.",
    NULL,
    [](const Parameters& paramters) -> Fetcher::Plugin* {
      CurlFetcherPlugin::Flags flags;
      Try<Owned<Fetcher::Plugin>> result = CurlFetcherPlugin::create(flags);

      if (result.isError()) {
        return NULL;
      }

      return result.get().get();
    });
