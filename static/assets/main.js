/*
    JavaScript autoComplete v1.0.0 beta
    Copyright (c) 2014 Simon Steinberger / Pixabay
    GitHub: https://github.com/Pixabay/JavaScript-autoComplete
    License: http://www.opensource.org/licenses/mit-license.php
*/

var autoComplete = (function(){
    // "use strict";
    function autoComplete(options){
        if (!document.querySelector) return;

        // helpers
        function hasClass(el, className){ return el.classList ? el.classList.contains(className) : new RegExp('\\b'+ className+'\\b').test(el.className); }

        function triggerEvent(el, type){
            if (document.createEvent) { var e = document.createEvent('HTMLEvents'); e.initEvent(type, true, true); el.dispatchEvent(e); }
            else { var e = document.createEventObject(); e.eventType = type; el.fireEvent('on'+e.eventType, e); }
        }
        function addEvent(el, type, handler){
            if (el.attachEvent) el.attachEvent('on'+type, handler); else el.addEventListener(type, handler);
        }
        function removeEvent(el, type, handler){
            // if (el.removeEventListener) not working in IE11
            if (el.detachEvent) el.detachEvent('on'+type, handler); else el.removeEventListener(type, handler);
        }
        function live(elClass, event, cb, context){
            addEvent(context || document, event, function(e){
                var found, el = e.target || e.srcElement;
                while (el && !(found = hasClass(el, elClass))) el = el.parentElement;
                if (found) cb.call(el, e);
            });
        }

        var o = {
            selector: 0,
            source: 0,
            minChars: 3,
            delay: 150,
            cache: 1,
            menuClass: '',
            renderItem: function (item, search){
                var re = new RegExp("(" + search.split(' ').join('|') + ")", "gi");
                return '<div class="autocomplete-suggestion" data-val="' + item + '">' + item.replace(re, "<b>$1</b>") + '</div>';
            },
            onSelect: function(e, term, item){}
        };
        for (var k in options) { if (options.hasOwnProperty(k)) o[k] = options[k]; }

        // init
        var elems = typeof o.selector == 'object' ? [o.selector] : document.querySelectorAll(o.selector);
        for (var i=0; i<elems.length; i++) {
            var that = elems[i];

            // create suggestions container "sc"
            that.sc = document.createElement('div');
            that.sc.className = 'autocomplete-suggestions '+o.menuClass;

            that.setAttribute('data-sc', that.sc);
            that.autocompleteAttr = that.getAttribute('autocomplete');
            that.setAttribute('autocomplete', 'off');
            that.cache = {};
            that.last_val = '';

            that.updateSC = function(resize, next){
                var rect = that.getBoundingClientRect(),
                    scrollTop = (document.documentElement && document.documentElement.scrollTop) || document.body.scrollTop,
                    scrollLeft = (document.documentElement && document.documentElement.scrollLeft) || document.body.scrollLeft;
                that.sc.style.left = rect.left + scrollLeft + 'px';
                that.sc.style.top = rect.bottom + scrollTop + 1 + 'px';
                that.sc.style.width = rect.right - rect.left + 'px'; // outerWidth

                if (!resize) {
                    that.sc.style.display = 'block';
                    if (!that.sc.maxHeight) { that.sc.maxHeight = parseInt((window.getComputedStyle ? getComputedStyle(that.sc, null) : that.sc.currentStyle).maxHeight); }
                    if (!that.sc.suggestionHeight) {
                        var rect2 = that.sc.querySelector('.autocomplete-suggestion').getBoundingClientRect();
                        that.sc.suggestionHeight = rect2.bottom - rect2.top;
                    }
                    if (that.sc.suggestionHeight)
                        if (!next) that.sc.scrollTop = 0;
                        else {
                            var scrTop = that.sc.scrollTop, selTop = next.getBoundingClientRect().top - that.sc.getBoundingClientRect().top;
                            if (selTop + that.sc.suggestionHeight - that.sc.maxHeight > 0)
                                that.sc.scrollTop = selTop + that.sc.suggestionHeight + scrTop - that.sc.maxHeight;
                            else if (selTop < 0)
                                that.sc.scrollTop = selTop + scrTop;
                        }
                }
            }
            addEvent(window, 'resize', that.updateSC);
            document.body.appendChild(that.sc);

            live('autocomplete-suggestion', 'mouseleave', function(e){
                var sel = that.sc.querySelector('.autocomplete-suggestion.selected');
                if (sel) setTimeout(function(){ sel.className = sel.className.replace('selected', ''); }, 20);
            }, that.sc);

            live('autocomplete-suggestion', 'mouseover', function(e){
                var sel = that.sc.querySelector('.autocomplete-suggestion.selected');
                if (sel) sel.className = sel.className.replace('selected', '');
                this.className += ' selected';
            }, that.sc);

            live('autocomplete-suggestion', 'mouseup', function(e){
                if (hasClass(this, 'autocomplete-suggestion')) { // else outside click
                    var v = this.getAttribute('data-val');
                    const nodes = [];
                    for (var i = 0; i < that.childNodes; i++) {
                        if (that.childNodes[i].nodeName != '#text')
                            nodes.push(that.childNodes[i]);
                    }
                    console.log(v, nodes);
                    that.innerHTML = '';
                    nodes.forEach(function(el) { that.appendChild(el); });
                    that.innerHTML += "<b>" + v + "</b>";
                    o.onSelect(e, v, this);
                    that.focus();
                    that.sc.style.display = 'none';
                }
            }, that.sc);

            that.blurHandler = function(){
                try { var over_sb = document.querySelector('.autocomplete-suggestions:hover'); } catch(e){ var over_sb = 0; }
                if (!over_sb) {
                    that.last_val = that.value;
                    that.sc.style.display = 'none';
                } else that.focus();
            };
            addEvent(that, 'blur', that.blurHandler);

            that.focusHandler = function(){
                that.last_val = '\n';
                triggerEvent(that, 'keyup');
            };
            if (!o.minChars) addEvent(that, 'focus', that.focusHandler);

            var suggest = function(data){
                var val = that.value;
                that.cache[val] = data;
                if (data.length && val.length >= o.minChars) {
                    var s = '';
                    for (var i=0;i<data.length;i++) s += o.renderItem(data[i], val);
                    that.sc.innerHTML = s;
                    that.updateSC(0);
                }
                else
                    that.sc.style.display = 'none';
            }

            that.keydownHandler = function(e){
                var key = window.event ? e.keyCode : e.which;
                // down (40), up (38)
                if ((key == 40 || key == 38) && that.sc.innerHTML) {
                    var next, sel = that.sc.querySelector('.autocomplete-suggestion.selected');
                    if (!sel) {
                        next = (key == 40) ? that.sc.querySelector('.autocomplete-suggestion') : that.sc.childNodes[that.sc.childNodes.length - 1]; // first : last
                        next.className += ' selected';
                        that.value = next.getAttribute('data-val');
                    } else {
                        next = (key == 40) ? sel.nextSibling : sel.previousSibling;
                        if (next) {
                            sel.className = sel.className.replace('selected', '');
                            next.className += ' selected';
                            that.value = next.getAttribute('data-val');
                        }
                        else { sel.className = sel.className.replace('selected', ''); that.value = that.last_val; next = 0; }
                    }
                    that.updateSC(0, next);
                    return false;
                }
                // esc
                else if (key == 27) { that.value = that.last_val; that.sc.style.display = 'none'; }
                // enter
                else if (key == 13) {
                    var sel = that.sc.querySelector('.autocomplete-suggestion.selected');
                    if (sel) { o.onSelect(e, sel.getAttribute('data-val'), sel); setTimeout(function(){ that.focus(); that.sc.style.display = 'none'; }, 10); }
                }
            };
            addEvent(that, 'keydown', that.keydownHandler);

            that.keyupHandler = function(e){
                var key = window.event ? e.keyCode : e.which;
                if (key != 27 && key != 38 && key != 40 && key != 37 && key != 39) {
                    const nodes = that.childNodes;
                    var val = nodes.length > 0 ? nodes[nodes.length - 1].textContent : '';
                    that.value = val;
                    if (val.length >= o.minChars) {
                        if (val != that.last_val) {
                            that.last_val = val;
                            clearTimeout(that.timer);
                            if (o.cache) {
                                if (val in that.cache) { suggest(that.cache[val]); return; }
                                // no requests if previous suggestions were empty
                                for (var i=1; i<val.length-o.minChars; i++) {
                                    var part = val.slice(0, val.length-i);
                                    if (part in that.cache && !that.cache[part].length) { suggest([]); return; }
                                }
                            }
                            that.timer = setTimeout(function(){ o.source(val, suggest) }, o.delay);
                        }
                    } else {
                        that.last_val = val;
                        that.sc.style.display = 'none';
                    }
                }
            };
            addEvent(that, 'keyup', that.keyupHandler);
        }

        // public destroy method
        this.destroy = function(){
            for (var i=0; i<elems.length; i++) {
                var that = elems[i];
                removeEvent(window, 'resize', that.updateSC);
                removeEvent(that, 'blur', that.blurHandler);
                removeEvent(that, 'focus', that.focusHandler);
                removeEvent(that, 'keydown', that.keydownHandler);
                removeEvent(that, 'keyup', that.keyupHandler);
                if (that.autocompleteAttr)
                    that.setAttribute('autocomplete', that.autocompleteAttr);
                else
                    that.removeAttribute('autocomplete');
                document.body.removeChild(that.sc);
                that = null;
            }
        };
    }
    return autoComplete;
})();

(function(){
    if (typeof define === 'function' && define.amd)
        define('autoComplete', function () { return autoComplete; });
    else if (typeof module !== 'undefined' && module.exports)
        module.exports = autoComplete;
    else
        window.autoComplete = autoComplete;
})();

window.onload = function() {
    $('.tag-search-input').each(function(_, div) {
        div.style.lineHeight = '40px';

        const el = document.createElement('div');
        el.style.border = 'solid 2px #233';
        el.setAttribute('contenteditable', true);
        el.className = 'tag-box';
        // el.innerHTML = "<svg viewBox='0 0 16 16'><path d='M 2 8 L 14 8' stroke='#666' stroke-width=3 /><path d='M 8 2 L 8 14' stroke='#666' stroke-width=3 /></svg>";

        function select(src) {
            const t = $("<div>").
                addClass('tag-box normal user-selected').
                text(src.text());
            t.append($("<svg class=closer viewBox='0 0 16 16'><circle cx=8 cy=8 r=8 fill='#aaa' /><path d='M 5 5 L 11 11' stroke=white stroke-width=3 fill=transparent /><path d='M 11 5 L 5 11' stroke=white stroke-width=3 fill=transparent /></svg>").
                click(function() {
                    t.remove();
                })
            ).insertBefore(el);
            reset();
            el.innerText = '';
            el.focus();
        }

        function reset() {
            $(div).find('.candidate').remove();
            div.selector = 0;
            div.candidates = [];
        }

        el.oninput = function(e){
            const val = this.textContent;
            const that = this;
            if (val.length < 1) {
                $(div).find('.candidate').remove();
                return;
            }
            if (this.timer) clearTimeout(this.timer);
            this.timer = setTimeout(function(){
                if (that.textContent != val) return;
                $.get('/tag/search?q=' + encodeURIComponent(val), function(data) {
                    if (that.textContent != val) return;
                    
                    reset();
                    data.tags.forEach(function(tag, i) {
                        const t = $("<div>").
                            addClass('candidate tag-box ' + (i == 0 ? 'selected' : '')).
                            css('border', 'dotted 1px #666').
                            css('color', '#666').
                            css('cursor', 'pointer').
                            attr('tag-id', tag[0]).
                            text(tag[1]);
                        $(div).append(t.click(function() { select(t) }));
                        div.candidates.push(t);
                    })

                    console.log(new Date(), val, data.tags.length);
                });
            }, 200);
        }
        el.onkeydown = function(e) {
            if (e.keyCode == 9 && div.candidates.length) {
                const current = div.selector;
                div.selector = (div.selector + (e.shiftKey ? -1 : 1) + div.candidates.length) % div.candidates.length;
                div.candidates[current].removeClass('selected');
                div.candidates[div.selector].addClass('selected');
                e.preventDefault();
            }
            if (e.keyCode == 13) {
                if (div.candidates.length) {
                    select(div.candidates[div.selector]);
                }
                e.preventDefault();
            }
        }
        div.appendChild(el);
    })
}
