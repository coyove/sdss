window.onload = function() {
    $('.tag-search-input-container').each(function(_, container) {
        const div = document.createElement('div');
        div.className = 'tag-search-input';

        const el = document.createElement('div');
        el.setAttribute('contenteditable', true);
        el.className = 'tag-box tag-search-box';
        el.style.outline = 'none';
        el.style.minWidth = '2em';

        const loader = $("<div class=tag-box style='min-width:2em;padding:0'><div class=lds-dual-ring></div></div>").get(0);

        const selected = {};
        function select(src, fromHistory) {
            const tagID = parseInt(src.attr('tag-id'));
            if (!(tagID in selected)) {
                selected[tagID] = src.text();
                const t = $("<div>").
                    addClass('tag-box normal user-selected').
                    attr('tag-id', tagID).
                    append($("<span>").text(src.text()));
                t.append($("<svg class=closer viewBox='0 0 16 16'><circle cx=8 cy=8 r=8 fill='#aaa' /><path d='M 5 5 L 11 11' stroke=white stroke-width=3 fill=transparent /><path d='M 11 5 L 5 11' stroke=white stroke-width=3 fill=transparent /></svg>").
                    click(function(ev) {
                        delete selected[tagID];
                        t.remove();
                        el.focus();
                        ev.stopPropagation();
                    })
                );
                if (fromHistory) {
                    t.insertBefore(src);
                    src.remove();
                } else {
                    t.insertBefore(el);
                }

                const history = JSON.parse(window.localStorage.getItem('tags-history') || '{}');
                history[tagID] = {'tag': src.text(), 'ts': new Date().getTime()};
                if (Object.keys(history).length > 10) {
                    var min = Number.MAX_VALUE, minID = 0;
                    for (const k in history) {
                        if (history[k].ts < min) {
                            min = history[k].ts;
                            minID = k;
                        }
                    }
                    delete history[minID];
                }
                window.localStorage.setItem('tags-history', JSON.stringify(history));
            }

            if (fromHistory !== true) reset();
            el.innerText = '';
            el.focus();
        }

        function reset() {
            $(div).find('.candidate').remove();
            div.selector = 0;
            div.candidates = [];
            loader.style.visibility = 'hidden';
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
                loader.style.visibility = '';
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
                            append($("<span>").text(tag[1]));
                        $(div).append(t.click(function(ev) {
                            select(t);
                            ev.stopPropagation();
                        }));
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
            if (e.keyCode == 8 && el.textContent.length == 0) {
                const last = $(div).find('.user-selected:last');
                delete selected[parseInt(last.attr('tag-id'))];
                last.remove();
                e.preventDefault();
            }
            if (e.keyCode == 27) {
                el.innerHTML = '';
                reset();
                e.preventDefault();
            }
        }
        el.onblur = function() {
            if (this.blurtimer) clearTimeout(this.blurtimer);
            this.blurtimer = setTimeout(function() {
                el.innerHTML = '';
                reset();
            }, 1000);
        }
        el.onfocus = function() {
            if (this.blurtimer) clearTimeout(this.blurtimer);
        }
        container.onmouseup = function(ev) {
            el.focus();
            ev.preventDefault();
        }

        div.appendChild(el);
        div.appendChild(loader);
        container.appendChild(div);
        reset();

        const history = JSON.parse(window.localStorage.getItem('tags-history') || '{}');
        for (const k in history) {
            const t = $("<div>").
                addClass('candidate tag-box').
                css('border', 'dotted 1px #666').
                css('color', '#666').
                css('cursor', 'pointer').
                attr('tag-id', k).
                append($("<span>").text(history[k].tag));
            t.click(function(ev) {
                select(t, true);
                ev.stopPropagation();
            }).insertBefore(el);
            div.candidates.push(t);
        }
    })
}
