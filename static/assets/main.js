window.CONST_closeSVG = "<svg class='svg16 closer' viewBox='0 0 16 16'><circle cx=8 cy=8 r=8 fill='#aaa' /><path d='M 5 5 L 11 11' stroke=white stroke-width=3 fill=transparent /><path d='M 11 5 L 5 11' stroke=white stroke-width=3 fill=transparent /></svg>";
window.CONST_tickSVG = "<svg class='svg16' viewBox='0 0 16 16'><circle cx=8 cy=8 r=8 fill='green' /><path fill=white stroke=transparent d='M 4.139 6.749 L 2.235 8.848 L 6.491 13.156 L 13.534 6.429 L 11.657 4.382 L 6.781 9.244 L 4.139 6.749 Z' /></svg>";
window.CONST_starSVG = "<svg class='svg16 starer' viewBox='0 0 16 16'><circle cx=8 cy=8 r=8 fill='#aaa' /><path d='M 8.065 2.75 L 9.418 6.642 L 13.537 6.726 L 10.254 9.215 L 11.447 13.159 L 8.065 10.806 L 4.683 13.159 L 5.876 9.215 L 2.593 6.726 L 6.712 6.642 Z' fill=white /></svg>";
window.CONST_loaderHTML = "<div class=lds-dual-ring></div>";

function clickAjax(el, path, argsf, f, config) {
    $(el).click(function() {
        if (config && config.ask) {
            if (!confirm(config.ask)) return;
        }

        var url = path + '?ajax=1';
        const args = argsf();
        for (const k in args) url += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(args[k]);
        const that = $(this);
        const rect = this.getBoundingClientRect();
        const loader = $("<div style='display:inline-block;text-align:center'>" + window.CONST_loaderHTML + "</div>").
            css('width', rect.width + 'px');
        that.hide();
        loader.insertBefore(that);
        $.post(url, function(data) {
            f(data);
        }).always(function() {
            that.show();
            loader.remove();
        });
    })
}

window.onload = function() {
    $('.tag-search-input-container').each(function(_, container) {
        const editable = $(container).attr('edit') == 'edit';
        const maxTags = parseInt($(container).attr('max-tags') || '99');
        const div = document.createElement('div');
        div.className = 'tag-search-input';

        const el = document.createElement('div');
        el.setAttribute('contenteditable', true);
        el.className = 'tag-box tag-search-box';
        el.style.outline = 'none';
        el.style.minWidth = '2em';
        el.style.flexGrow = '1';
        el.style.justifyContent = 'left';

        const loader = $("<div class=tag-box style='min-width:2em;padding:0'>" + window.CONST_loaderHTML + "</div>").get(0);

        const info = $("<div class=tag-box style='font-size:80%;color:#aaa'></div>").get(0);

        const selected = {};

        function updateInfo() {
            const sz = Object.keys(selected).length;
            info.innerText = sz + '/' + maxTags;
            el.setAttribute('contenteditable', sz < maxTags);
        }

        function select(src, fromHistory) {
            const tagID = parseInt(src.attr('tag-id'));
            if (!(tagID in selected) && Object.keys(selected).length < maxTags) {
                selected[tagID] = {'tag': src.text()};
                const t = $("<div>").addClass('tag-box normal user-selected').attr('tag-id', tagID);
                if (editable) {
                    t.append($(window.CONST_starSVG).click(function(ev){
                        t.toggleClass('tag-required');
                        selected[tagID].required = t.hasClass('tag-required');
                    }));
                }
                t.append($("<span>").text(src.text()));
                t.append($(window.CONST_closeSVG).click(function(ev) {
                    delete selected[tagID];
                    t.remove();
                    updateInfo();
                    el.focus();
                    ev.stopPropagation();
                }));
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
                updateInfo();
            }

            if (fromHistory !== true) reset();
            el.innerText = '';
            el.focus();
        }

        function reset() {
            $(div).find('.candidate').remove();
            div.selector = 0;
            div.candidates = [];
            loader.style.display = 'none';
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
                loader.style.display = '';
                $.get('/tag/search?q=' + encodeURIComponent(val), function(data) {
                    if (that.textContent != val) return;
                    
                    reset();
                    data.tags.forEach(function(tag, i) {
                        const t = $("<div>").
                            addClass('candidate tag-box ' + (i == 0 ? 'selected' : '')).
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
            if ((e.keyCode == 9 || e.keyCode == 39) && div.candidates.length) {
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
                $(div).find('.user-selected:last .closer').click();
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
        div.appendChild(info);
        div.appendChild(loader);
        container.appendChild(div);
        reset();

        const history = JSON.parse(window.localStorage.getItem('tags-history') || '{}');
        for (var i = 0; ; i++) {
            const data = $(container).attr('tag-data' + i);
            if (!data) break;
            history[parseInt(data.split(',')[0])] = {'tag': data.split(',')[1]};
        }
        for (const k in history) {
            const t = $("<div>").
                addClass('candidate tag-box').
                attr('tag-id', k).
                append($("<span>").text(history[k].tag));
            t.click(function(ev) {
                select(t, true);
                ev.stopPropagation();
            }).insertBefore(el);
            div.candidates.push(t);
        }

        updateInfo();
        container.getTags = function() { return selected; }
    })

    function teIcon(src) { return $(src).css('margin-left','0.25em'); }
    $('.tag-edit').each(function(_, el) {
        const input = $(el).find('input');
        const tagID = $(el).attr('tag-id');
        const path = '/tag/manage/action';
        if (tagID) {
            input.get(0).onfocus = function() {
                $(el).find('.display-row').css('overflow', 'visible');
            }
            input.get(0).onblur = function() {
                $(el).find('.display-row').css('overflow', 'hidden');
            }
            clickAjax($(el).find('[action=update]').append(teIcon(window.CONST_tickSVG)), path, function() {
                return {
                    'action': 'update',
                    'id': tagID, 
                    'text': input.val(),
                };
            }, function(data) {
                if (data.success) {
                } else {
                    alert(data.code);
                }
            });
            clickAjax($(el).find('[action=delete]').append(teIcon(window.CONST_closeSVG)), path, function() {
                return {'action': 'delete', 'id': tagID};
            }, function(data) {
                if (data.success) {
                    $('#tag' + tagID).remove();
                } else {
                    alert(data.code);
                }
            }, {'ask': '确认删除 ' + input.val()});
            ['approve', 'reject'].forEach(function(action) { 
                clickAjax($(el).find('[action=' + action + ']').
                    append(teIcon(action == 'approve' ? window.CONST_tickSVG : window.CONST_closeSVG)),
                    path, function() {
                        return {'action':action, 'id': tagID};
                    }, function(data) {
                        if (data.success) {
                            $('#tag' + tagID).remove();
                        } else {
                            alert(data.code);
                        }
                    });
            });
        }
        clickAjax($(el).find('[action=create]'), path, function() {
            return {'action': 'create', 'text': input.val()};
        }, function(data) {
            if (data.success) {
                location.href = ('?sort=0&desc=1');
            } else {
                alert(data.code);
            }
        });
    });
}
