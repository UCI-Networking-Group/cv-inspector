
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

(function() {
    const fileNameSuffix = "--cvdommutationvanilla";
    const portName = "dommutation";
    const urlCheck = {}
    const mainLog = {}
    const networkFilters = {
        urls: [
        ]
    };

    /*! @source http://purl.eligrey.com/github/FileSaver.js/blob/master/FileSaver.js */
    var saveAs = saveAs || (navigator.msSaveBlob && navigator.msSaveBlob.bind(navigator)) || (function (h) { var r = h.document, l = function () { return h.URL || h.webkitURL || h }, e = h.URL || h.webkitURL || h, n = r.createElementNS("http://www.w3.org/1999/xhtml", "a"), g = "download" in n, j = function (t) { var s = r.createEvent("MouseEvents"); s.initMouseEvent("click", true, false, h, 0, 0, 0, 0, 0, false, false, false, false, 0, null); t.dispatchEvent(s) }, o = h.webkitRequestFileSystem, p = h.requestFileSystem || o || h.mozRequestFileSystem, m = function (s) { (h.setImmediate || h.setTimeout)(function () { throw s }, 0) }, c = "application/octet-stream", k = 0, b = [], i = function () { var t = b.length; while (t--) { var s = b[t]; if (typeof s === "string") { e.revokeObjectURL(s) } else { s.remove() } } b.length = 0 }, q = function (t, s, w) { s = [].concat(s); var v = s.length; while (v--) { var x = t["on" + s[v]]; if (typeof x === "function") { try { x.call(t, w || t) } catch (u) { m(u) } } } }, f = function (t, u) { var v = this, B = t.type, E = false, x, w, s = function () { var F = l().createObjectURL(t); b.push(F); return F }, A = function () { q(v, "writestart progress write writeend".split(" ")) }, D = function () { if (E || !x) { x = s(t) } if (w) { w.location.href = x } v.readyState = v.DONE; A() }, z = function (F) { return function () { if (v.readyState !== v.DONE) { return F.apply(this, arguments) } } }, y = { create: true, exclusive: false }, C; v.readyState = v.INIT; if (!u) { u = "download" } if (g) { x = s(t); n.href = x; n.download = u; j(n); v.readyState = v.DONE; A(); return } if (h.chrome && B && B !== c) { C = t.slice || t.webkitSlice; t = C.call(t, 0, t.size, c); E = true } if (o && u !== "download") { u += ".download" } if (B === c || o) { w = h } else { w = h.open() } if (!p) { D(); return } k += t.size; p(h.TEMPORARY, k, z(function (F) { F.root.getDirectory("saved", y, z(function (G) { var H = function () { G.getFile(u, y, z(function (I) { I.createWriter(z(function (J) { J.onwriteend = function (K) { w.location.href = I.toURL(); b.push(I); v.readyState = v.DONE; q(v, "writeend", K) }; J.onerror = function () { var K = J.error; if (K.code !== K.ABORT_ERR) { D() } }; "writestart progress write abort".split(" ").forEach(function (K) { J["on" + K] = v["on" + K] }); J.write(t); v.abort = function () { J.abort(); v.readyState = v.DONE }; v.readyState = v.WRITING }), D) }), D) }; G.getFile(u, { create: false }, z(function (I) { I.remove(); H() }), z(function (I) { if (I.code === I.NOT_FOUND_ERR) { H() } else { D() } })) }), D) }), D) }, d = f.prototype, a = function (s, t) { return new f(s, t) }; d.abort = function () { var s = this; s.readyState = s.DONE; q(s, "abort") }; d.readyState = d.INIT = 0; d.WRITING = 1; d.DONE = 2; d.error = d.onwritestart = d.onprogress = d.onwrite = d.onabort = d.onerror = d.onwriteend = null; h.addEventListener("unload", i, false); return a }(self));
 
    function sleep(seconds) {
        var start = new Date().getTime();
        while (new Date() < start + seconds * 1000) { }
        return 0;
    }

    function sendMessageToContent(){   
        chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
            chrome.tabs.sendMessage(tabs[0].id, {greeting: "hello"}, function(response) {});
        });
        
    }

    chrome.tabs.onUpdated.addListener(onUpdated);

    function onUpdated(tabId, changeInfo, tab) {
    
        // upon loading a new url, it will try to save the current requests that we have
        if(!tab.url.includes('chrome') && !tab.url.includes('dev') && !tab.url.includes('newtab')) {
            // if urlCheck does not have tabId yet
            if (!urlCheck.hasOwnProperty(tabId)) {
                console.log("General Starting a new tab in onUpdated: " + tabId);
                urlCheck[tabId] = {url: '', loading: false, filename: ''};
                mainLog[tabId] = [];
            }

            // if the tab is loading and this is a new loading status, then proceed. 
            // Or if the tab wants to load a new url then let it
            if(changeInfo.hasOwnProperty("status") && typeof changeInfo.status === 'undefined'){
                changeInfo.status = 'loading';
            }
            console.log("General ChangeInfo: " + changeInfo.status);
            if(changeInfo.status == 'loading' && (urlCheck[tabId].url != tab.url) ) {
                console.log("General: onUpdated 1");
                if(urlCheck[tabId].url != '') {
                    console.log("General: onUpdated 2: Saving File");

                    var fileData = {
                        url: urlCheck[tabId].url,
                        dommutation: mainLog[tabId]
                    }

                    if (fileData.dommutation.length > 0) {
                        // take the first event as the starttime of the original url
                        var firstEvent = fileData.dommutation[0];
                        fileData.startTime = firstEvent.time;
                    } else {
                        fileData.startTime = "";
                    }
                    // set the endTime now as when we start loading the next url
                    fileData.endTime = new Date().getTime();
                    
                    var blob = new Blob([JSON.stringify(fileData)], { type: "application/json;charset=utf-8" });
                    var filename = urlCheck[tabId].filename;
                    if (filename == null || filename.length== 0){
                        filename = urlCheck[tabId].url;
                    }
                    saveAs(blob, filename + fileNameSuffix + '.json');
                }
                console.log("General: onUpdated 3: Starting a new:" + " tabid: " + tabId + ", url: " + tab.url);

                // start new for the tabId and tab.url
                urlCheck[tabId].url = tab.url;
                urlCheck[tabId].loading = true;
                urlCheck[tabId].filename = '';
                mainLog[tabId] = [];

                logEventBackground('onTabNewURL', {"url": tab.url, "tabId": tabId}, tabId);

                console.log(JSON.stringify({tabId: tabId, url: urlCheck[tabId].url, type: 'Page Loading',time: new Date().getTime()}))
            
            } else if (changeInfo.status == 'complete' && urlCheck[tabId].url == tab.url) {
                // complete status only changes if complete AND the url of the tab stays the same
                let tabUrl = '';
                if (urlCheck.hasOwnProperty(tabId)) {
                    tabUrl = urlCheck[tabId].url;
                }
                urlCheck[tabId].loading = false;
                console.log(JSON.stringify({tabId: tabId, url: tabUrl, type: 'Page Completed', time: new Date().getTime()}))
            }
        }  

    }


    function logEventBackground(eventName, eventData, tabId) {

        // if log does not have tabId yet
        if (!mainLog.hasOwnProperty(tabId)) {
            console.log("General: Found a new tab for logging: " + tabId);
            mainLog[tabId] = []
        }

        console.log("General: Adding event " + eventName + " to tabId: " + tabId);

        mainLog[tabId].push({
            type: eventName,
            event: eventData,
            time: new Date().getTime()
        });
    }

    function checkMainLog(tabId) {
        if (!mainLog.hasOwnProperty(tabId)) {
            console.log("General: Creating mainLog for tab: " + tabId);
            mainLog[tabId] = [];
        } 
    }

    chrome.tabs.onActivated.addListener((tab) => {
        const tabId = tab ? tab.tabId : chrome.tabs.TAB_ID_NONE;
        checkMainLog(tabId);
        logEventBackground('onTabActivated', {"tabId": tabId}, tabId);
    });

    chrome.tabs.onRemoved.addListener((tab) => {
        const tabId = tab.tabId;
        if (!checkMainLog.hasOwnProperty(tabId)) {
            return;
        }
        logEventBackground('onTabRemoved', {"tabId": tabId}, tabId);
    });

    function handleContentScriptConnection(port) {
        const tabId = port.sender.tab.id;
        checkMainLog(tabId);
    
        var messageListener = function (message, sender, sendResponse) {

            mainLog[tabId].push(message);
            console.log(JSON.stringify(message));

            // handle special custom event
            if (message.type == "AnticvFileNameEvent") {
                //console.log("Setting filename " + message.event.filename)
                urlCheck[tabId].filename = message.event.filename;
            }
            
        };
        port.onMessage.addListener(messageListener);
    }

    chrome.runtime.onConnect.addListener(function (port) {
        if (port.name === portName) {
            handleContentScriptConnection(port);
        } 
    });
    

}());