
/*
 * Copyright (c) 2021 Hieu Le and the UCI Networking Group
 * <https://athinagroup.eng.uci.edu>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const portName = "cvwebrequests_variant";

if(document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', fireContentLoadedEvent, false);
} else {
    fireContentLoadedEvent();
}

function fireContentLoadedEvent () {
    console.log("DOMContentLoaded");
    domLoaded = true;
    window.addEventListener("AnticvFileNameEvent", function(event) {
        port.postMessage({
            type: event.type,
            event: event.detail
        });
    }, true);
}

var port = chrome.runtime.connect({name: portName});

port.postMessage({
    type: 'connected----'+document.URL
});
