// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use await_tree::Registry;
use lazy_static::lazy_static;

use std::sync::Arc;

use tokio::sync::Mutex;

lazy_static! {
    pub static ref AWAIT_TREE_REGISTRY: Arc<Mutex<Registry<u64>>> =
        Arc::new(Mutex::new(Registry::new(await_tree::Config::default())));
}
