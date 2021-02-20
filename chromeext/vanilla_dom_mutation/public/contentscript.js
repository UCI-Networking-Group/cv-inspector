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

/*! @source http://purl.eligrey.com/github/FileSaver.js/blob/master/FileSaver.js */
var saveAs = saveAs || (navigator.msSaveBlob && navigator.msSaveBlob.bind(navigator)) || (function (h) { var r = h.document, l = function () { return h.URL || h.webkitURL || h }, e = h.URL || h.webkitURL || h, n = r.createElementNS("http://www.w3.org/1999/xhtml", "a"), g = "download" in n, j = function (t) { var s = r.createEvent("MouseEvents"); s.initMouseEvent("click", true, false, h, 0, 0, 0, 0, 0, false, false, false, false, 0, null); t.dispatchEvent(s) }, o = h.webkitRequestFileSystem, p = h.requestFileSystem || o || h.mozRequestFileSystem, m = function (s) { (h.setImmediate || h.setTimeout)(function () { throw s }, 0) }, c = "application/octet-stream", k = 0, b = [], i = function () { var t = b.length; while (t--) { var s = b[t]; if (typeof s === "string") { e.revokeObjectURL(s) } else { s.remove() } } b.length = 0 }, q = function (t, s, w) { s = [].concat(s); var v = s.length; while (v--) { var x = t["on" + s[v]]; if (typeof x === "function") { try { x.call(t, w || t) } catch (u) { m(u) } } } }, f = function (t, u) { var v = this, B = t.type, E = false, x, w, s = function () { var F = l().createObjectURL(t); b.push(F); return F }, A = function () { q(v, "writestart progress write writeend".split(" ")) }, D = function () { if (E || !x) { x = s(t) } if (w) { w.location.href = x } v.readyState = v.DONE; A() }, z = function (F) { return function () { if (v.readyState !== v.DONE) { return F.apply(this, arguments) } } }, y = { create: true, exclusive: false }, C; v.readyState = v.INIT; if (!u) { u = "download" } if (g) { x = s(t); n.href = x; n.download = u; j(n); v.readyState = v.DONE; A(); return } if (h.chrome && B && B !== c) { C = t.slice || t.webkitSlice; t = C.call(t, 0, t.size, c); E = true } if (o && u !== "download") { u += ".download" } if (B === c || o) { w = h } else { w = h.open() } if (!p) { D(); return } k += t.size; p(h.TEMPORARY, k, z(function (F) { F.root.getDirectory("saved", y, z(function (G) { var H = function () { G.getFile(u, y, z(function (I) { I.createWriter(z(function (J) { J.onwriteend = function (K) { w.location.href = I.toURL(); b.push(I); v.readyState = v.DONE; q(v, "writeend", K) }; J.onerror = function () { var K = J.error; if (K.code !== K.ABORT_ERR) { D() } }; "writestart progress write abort".split(" ").forEach(function (K) { J["on" + K] = v["on" + K] }); J.write(t); v.abort = function () { J.abort(); v.readyState = v.DONE }; v.readyState = v.WRITING }), D) }), D) }; G.getFile(u, { create: false }, z(function (I) { I.remove(); H() }), z(function (I) { if (I.code === I.NOT_FOUND_ERR) { H() } else { D() } })) }), D) }), D) }, d = f.prototype, a = function (s, t) { return new f(s, t) }; d.abort = function () { var s = this; s.readyState = s.DONE; q(s, "abort") }; d.readyState = d.INIT = 0; d.WRITING = 1; d.DONE = 2; d.error = d.onwritestart = d.onprogress = d.onwrite = d.onabort = d.onerror = d.onwriteend = null; h.addEventListener("unload", i, false); return a }(self));
a = []
html=''
const portName = "dommutation";
var nodeRegistry = [];
var nodeToParent = {};
var domLoaded = false;
var windowLoaded = false;

var ABP_BLOCKED_KEY = 'abp-blocked-element';
var ABP_SNIPPET_KEY = 'abp-blocked-snippet';

if(document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', fireContentLoadedEvent, false);
    window.addEventListener('load', fireWindowLoadedEvent, false);

} else {
    fireContentLoadedEvent();
}

function fireContentLoadedEvent () {
    console.log("DOMContentLoaded");

    domLoaded = true;
    // log the dom loaded event
    logEvent({
        "type":"DOMContentLoaded"
    });

    // this is used for selenium injected JS to trigger and event (unsafe but it works).
    window.addEventListener("AnticvFileNameEvent", function(event) {
        port.postMessage({
            type: event.type,
            event: event.detail
        });
    }, true);
}


function fireWindowLoadedEvent() {
    console.log("winwdow load event");
    windowLoaded = true;
    
    logEvent({
        "type":"WindowLoaded"
    });
}

var actualCode = '(' + function() {
    document._oldGetElementById = document.getElementById;
    document.getElementById = function(elemIdOrName) {
    var result = document._oldGetElementById(elemIdOrName);
    if (! result) {
        var elems = document.getElementsByName(elemIdOrName); 
        if (elems && elems.length > 0) {
            result = elems[0];
        }
    }

    console.log(result)
    }
	
} + ')();';

var port = chrome.runtime.connect({name: portName});

port.postMessage({
    type: 'connected----'+document.URL
});

chrome.runtime.onMessage.addListener(messageListener);

function messageListener(){
	traverse(document.documentElement)
	
	logEvent({
		DOM :html,
		innerHTML: document.body.innerHTML,
		innerText: document.body.innerText
    });
}

 var NodePropertiesClass = function() {
 	id=''
 	NoChildNodes=0
 	NodeType=''
 	NodeValue=''
 	nodeName=''
 	localName=''
 	nextSibling=''
 	namespaceURI=''
 	previousSibling=''
 	namespaceURI=''
 	prefix=''
 	nextSibling=''
 	parentNode=''
 	NodesAttributes=[]
 	NodeInnerHTML=''
 };
  
 function traverse( node )
   {
      // Get the name of the node
      var line = node.nodeName ;
      
      // If it is a #TEXT node
      if( node.nodeType == 3 )
      {
         // Get the text
         var text = " " + node.nodeValue ;

         // Clean it up
         text     = text.replace( /[\r\n]/g, " " ) ;
         text     = text.replace( / +/g,     " " ) ;

         // And add it to the line
         line    += text ;
      }

      // Add the line to the text in the box
      html += line + "\n" ;

      // Get an array with the child nodes
      var children = node.childNodes ;

      // Traverse each of the child nodes
      for( var i = 0; i < children.length; i++ )
          traverse( children[i] ) ;
   }

 function getAttributesInfo(node){
 	var index, rv, attrs;
    
    rv = [];
    attrs = node.attributes;
    if (attrs != null) { 
    for (index = 0; index < attrs.length; ++index) {
      rv.push([attrs[index].nodeName,attrs[index].nodeValue]);
      }
    }
    rv.sort();
    return rv;
 	
 }
 function getNodeInfo(node){
 	
 	nodeProperties = new NodePropertiesClass()
    nodeProperties.id= node.id
 	nodeProperties.NoChildNodes= node.childElementCount
 	nodeProperties.NodeType= node.nodeType
 	nodeProperties.NodeValue=node.nodeValue
 	nodeProperties.nodeName= node.nodeName
 	nodeProperties.localName= node.localName
 	
 	nodeProperties.namespaceURI= node.namespaceURI
 	nodeProperties.parentNode= (node.parentNode) ? node.parentNode.localName : ''
 	nodeProperties.NodesAttributes=getAttributesInfo(node)
 	
 	
 	
 	return nodeProperties
 }
 function getStruct(node){
    let structList=[]
    let childNodes= node.childNodes
 	structList.push(getNodeInfo(node))
 	return structList
 }
 
 function nodesToInfo(nodes){
        return Array.prototype.map.call(nodes, function (node) {
            return getStruct(node);
        });
    }
 function nodeToSelector(node, contextNode) {
        if (node.id) {
            return '#' + node.id;
        } else if (node.classList && node.classList.length) {
            return node.tagName + '.' + Array.prototype.join.call(node.classList, '.');
        } else if (node.parentElement && node.parentElement !== contextNode) {
            var parentSelector = nodeToSelector(node.parentElement, contextNode);

            if (node.nodeName === '#comment') {
                return parentSelector + ' > (comment)';
            } else if (node.nodeName === '#text') {
                return parentSelector + ' > (text)';
            } else {
                return parentSelector + ' > ' + node.nodeName;
            }
        } else if (node.nodeName) {
            if (node.nodeName === '#comment') {
                return '(comment)';
            } else if (node.nodeName === '#text') {
                return '(text)';
            } else {
                return node.nodeName;
            }
        } else {
            return '(unknown)';
        }
    }

function nodesToObjects(nodes, contextNode) {
        return Array.prototype.map.call(nodes, function (node) {
            return nodeToObject(node, contextNode);
        });
    }


function nodeToObject(node, contextNode) {
        var nodeId = nodeRegistry.indexOf(node);

        if (nodeId === -1) {
            nodeRegistry.push(node);
            nodeId = nodeRegistry.length - 1;
        }

        return {
            selector: nodeToSelector(node, contextNode),
            nodeId: nodeId
             
        };
    }

function hasABPBlocked(element) {
    if ("hasAttribute" in element) {
        return element.hasAttribute(ABP_BLOCKED_KEY);
    }

    return false;
}

function hasParentABPBlocked(elements, includeSelf) {
    result =  Array.prototype.map.call(elements, function (element) {
        // walk up the parent tree and see if it has attribute "abp-blocked"
        var match_attribute = false;
        var curr_element = element;
        if (includeSelf == true) {
            match_attribute = hasABPBlocked(curr_element);
        }
        while(curr_element.parentNode != null && !match_attribute) {
            match_attribute = hasABPBlocked(curr_element.parentNode);
            curr_element = curr_element.parentNode;
        }
        return match_attribute;
    });

    return result;
}

// TODO: collect all events from start. THen throw an event for documentloaded as well
 function logEvent(event) {
    // allow adb blocked events only before dom loaded

     if (!domLoaded && event.type == 'attribute changed' && event.attribute && event.attribute == ABP_BLOCKED_KEY) {
        port.postMessage({
            type: 'event',
            event: event ,
            time:  new Date().getTime()
        });
     }

     // allow adb snippets events only before dom loaded
     if (!domLoaded && event.type == 'nodes added') {
         // list of list
         let node_attributes = event.nodeInfo.NodesAttributes;
         if (node_attributes) {
             console.log("found attributes");
             console.log(node_attributes);
             // take first list only
             for (let i = 0 ; i < node_attributes[0].length; i++) {
                 let attr = node_attributes[i];
                 let attr_name = attr[0];
                 let attr_value = attr[1];
                 if (attr_name == "class" && attr_value.indexOf(ABP_SNIPPET_KEY) != -1 ) {
                     console.log("posted abp snippet event");
                    port.postMessage({
                        type: 'event',
                        event: event ,
                        time:  new Date().getTime()
                    }); 
                 }
             }
         }
     }
     
    if(domLoaded && 
        !document.URL.includes('chrome') && 
        !document.URL.includes('dev') && 
        !document.URL.includes('newtab')){

        port.postMessage({
            type: 'event',
            event: event ,
            time:  new Date().getTime()
        });
        
       }
    }

 function onMutation(records) {
        var record, i, l;

        for (i = 0, l = records.length; i < l; i++) {
            record = records[i];
            const targetElement = record.target;

            const targetNode = nodeToObject(targetElement);

            /**
             * Important: For "nodes added and nodes removed" the target node is the one that will have CHILDREN added to or removed from. 
             * It is NOT the elements being added or removed
             */

            if (record.type === 'childList') {
                if (record.addedNodes.length) {
                    // here the targetElement is the parent
                	//completeNode= JSON.stringify(record)
                	//addedNodes.push(record.addedNodes)
                    logEvent({
                        type: 'nodes added',
                        target: targetNode,
                        nodes: nodesToObjects(record.addedNodes, targetElement),
                        nodeInfo: nodesToInfo(record.addedNodes)
                    });
                }

                if (record.removedNodes.length) {
                    // here the targetElement is the parent
                    logEvent({
                        type: 'nodes removed',
                        target: targetNode,
                        nodes: nodesToObjects(record.removedNodes, targetElement),
                        nodeInfo: nodesToInfo(record.removedNodes)
                    });     
                }
            } else if (record.type === 'attributes') {
                const targetStruct = getStruct(targetElement);
                let event = {
                    type: 'attribute changed',
                    target: targetNode,
                    targetType: targetElement ? targetElement.nodeName: '',
                    parentNode: targetElement && targetElement.parentNode ? nodeToObject(targetElement.parentNode)["selector"]: '',
                    attribute: record.attributeName,
                    oldValue: record.oldValue,
                    newValue: targetElement.getAttribute(record.attributeName),
                    recd: targetStruct
                };

                if (targetElement) {
                    logEvent(event);
                }
                
                
            } else if (record.type === 'characterData') {
                logEvent({
                    type: 'text changed',
                    target: targetNode,
                    newValue: targetElement.data,
                    oldValue: record.oldValue
                });

           
            } else {
                console.error('DOM Listener Extension: unknown type of event', record);
            }
        }
    }

	
	
var MutationObserver = window.MutationObserver || window.WebKitMutationObserver;

    if (typeof MutationObserver !== 'function') {
        console.error('DOM Listener Extension: MutationObserver is not available in your browser.');
     
    }

    var observer = new MutationObserver(onMutation);
    var observerSettings = {
        subtree: true,
        childList: true,
        attributes: true,
        attributeOldValue: true,
        characterData: true,
        characterDataOldValue: true
    };	
	
	
	

function sleep(seconds) {
    var start = new Date().getTime();
    while (new Date() < start + seconds * 1000) { }
    return 0;
}

 function findShadowRoots(node, list) {
        list = list || [];

        if (node.shadowRoot) {
            list.push(node.shadowRoot);
        }

        if (node && node.querySelectorAll) {
            Array.prototype.forEach.call(node.querySelectorAll('*'), function (child) {
                if (true) {
                    findShadowRoots(child, list);
                }
            });
        }

        return list;
    }
    
    

observer.observe(document, observerSettings);
  
window.alert = function alert (message) {
        logEvent({
            type: 'nodes added',
            target: 'Page',
            nodes: ['Alert'],
            record: message
        });
    }

findShadowRoots(document).forEach(function (shadowRoot) {
                    observer.observe(shadowRoot, observerSettings);
                });

