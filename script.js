(function () {
    'use strict';
    var scope = (typeof container !== 'undefined' && container && container.querySelector) ? container : document;
    var app = scope.querySelector('#gdw-app');
    if (!app) return;
    var PID = (typeof pluginId !== 'undefined' && pluginId) || 'gdrive_watch';

    // 탭을 다시 열 때 이전 타이머 정리
    if (window.__gdwTimers) window.__gdwTimers.forEach(clearInterval);
    window.__gdwTimers = [];

    var ACTION = { create: '추가', edit: '수정', rename: '이동', move: '이동', delete: '삭제', restore: '복원' };
    var STATUS = { pending: '대기', waiting: '파일 대기', done: '반영됨', skipped: '보관함 밖', failed: '실패', timeout: '시간 초과' };
    var st = { status: '', q: '', page: 1, size: 50, total: 0, open: {}, picked: {}, tab: 'events', watch: null, alive: false };

    function $(sel) { return app.querySelector(sel); }
    function $$(sel) { return Array.prototype.slice.call(app.querySelectorAll(sel)); }
    function bind(name) { return app.querySelector('[data-bind="' + name + '"]'); }
    function el(tag, attrs, children) {
        var node = document.createElement(tag);
        Object.keys(attrs || {}).forEach(function (k) {
            if (k === 'text') node.textContent = attrs[k];
            else if (k === 'class') node.className = attrs[k];
            else if (k.slice(0, 2) === 'on') node.addEventListener(k.slice(2), attrs[k]);
            else if (attrs[k] !== undefined && attrs[k] !== null && attrs[k] !== false) node.setAttribute(k, attrs[k] === true ? '' : attrs[k]);
        });
        (children || []).forEach(function (c) { if (c) node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
        return node;
    }
    function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); return node; }
    function alive() { return document.body.contains(app); }
    function shortTime(s) { return s ? String(s).replace('T', ' ').slice(5, 16) : ''; }

    var toastTimer;
    function toast(text, isError) {
        var t = bind('toast');
        t.textContent = text;
        t.className = 'gdw-toast' + (isError ? ' is-error' : '');
        t.hidden = false;
        clearTimeout(toastTimer);
        toastTimer = setTimeout(function () { t.hidden = true; }, isError ? 7000 : 3500);
    }

    function rpc(action, context) {
        return fetch('/api/media/context-menu/book/plugins/action', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: 'general', plugin_id: PID, action_id: action, context: context || {} })
        }).then(function (res) {
            return res.json().catch(function () { return { success: false, error: 'HTTP ' + res.status }; });
        }).then(function (data) {
            if (!data || !data.success) throw new Error((data && (data.error || data.message)) || '요청 실패');
            return data;
        });
    }
    function act(action, context, reload) {
        return rpc(action, context).then(function (d) {
            if (d.message) toast(d.message);
            if (reload !== false) { loadStatus(); if (st.tab === 'events') loadEvents(); }
            return d;
        }).catch(function (e) { toast(e.message, true); });
    }

    // ───────── 현황 ─────────
    function loadStatus() {
        if (!alive()) return;
        rpc('status').then(renderStatus).catch(function (e) { bind('activity').textContent = '상태 조회 실패: ' + e.message; });
    }
    function renderStatus(d) {
        var w = d.worker || {};
        st.alive = !!w.alive;
        var pulse = bind('pulse');
        pulse.className = 'gdw-pulse' + (w.alive ? (w.activity && w.activity !== '대기 중' ? ' is-busy is-on' : ' is-on') : '');
        var text = w.alive ? ('감시 중 · ' + (w.activity || '')) : (w.stopped_by_user ? '중지됨 (직접 중지)' : '중지됨');
        if (w.alive && w.last_poll) text += ' · 마지막 확인 ' + shortTime(w.last_poll);
        if (w.alive && w.next_poll) text += ' · 다음 확인 ' + String(w.next_poll).slice(11, 16);
        if (w.error) text += ' · ' + w.error;
        bind('activity').textContent = text;
        bind('toggle').textContent = w.alive ? '중지' : '시작';

        var warn = clear(bind('warnings'));
        (d.warnings || []).forEach(function (m) { warn.appendChild(el('li', { text: m })); });
        warn.hidden = !(d.warnings || []).length;

        var c = d.counts || {};
        var grouped = { pending: (c.pending || 0) + (c.waiting || 0), done: c.done || 0, skipped: c.skipped || 0,
            failed: (c.failed || 0) + (c.timeout || 0) };
        ['pending', 'done', 'skipped', 'failed'].forEach(function (k) {
            app.querySelector('[data-count="' + k + '"]').textContent = grouped[k];
        });
        $$('.gdw-flow-seg').forEach(function (s) { s.classList.toggle('is-active', s.getAttribute('data-filter') === st.status); });
        var today = d.today || {};
        bind('meta').textContent = '오늘 ' + ((today.done || 0) + (today.skipped || 0)) + '건 처리, 실패 ' + ((today.failed || 0) + (today.timeout || 0)) +
            (c.waiting ? ' · 파일 대기 ' + c.waiting + '건' : '') +
            '건 · 보관함 ' + d.libraries + '개 인식' + (w.last_process ? ' · 마지막 처리 ' + shortTime(w.last_process) : '');

        var tbody = clear(bind('roots'));
        if (!(d.roots || []).length) {
            tbody.appendChild(el('tr', {}, [el('td', { colspan: 6, class: 'gdw-empty', text: '감시 중인 폴더가 없습니다. [감시 설정]에서 추가하세요.' })]));
        }
        (d.roots || []).forEach(function (r) {
            var cls = r.status === 'ready' ? 'ok' : (r.status === 'error' || r.status === 'blocked') ? 'fail' : 'wait';
            var label = !r.enabled ? '꺼짐' : r.status === 'ready' ? '정상' : r.status === 'blocked' ? '확인 필요로 중지' :
                r.status === 'error' ? '오류' : r.status === 'seeding' ? '정상' : '첫 확인 대기';
            var since = r.status === 'seeding' && r.updated ? Math.max(0, Math.round((Date.now() - new Date(r.updated).getTime()) / 60000)) : null;
            tbody.appendChild(el('tr', {}, [
                el('td', {}, [el('strong', { text: r.name }), el('small', { text: r.local_root })]),
                el('td', { text: r.fallback === 'userfeed' ? 'Drive · Changes (계정 전체 변경 목록)' : r.mode === 'activity' ? (r.fallback ? 'Drive · Changes (Activity 권한 없어 자동 전환)' : 'Drive · Activity') : r.mode === 'local' ? '로컬' + (r.local_detect === 'polling' ? ' · 주기' : r.local_detect === 'inotify' ? ' · 실시간' : '') : 'Drive · Changes' }),
                el('td', {}, [el('span', { class: 'gdw-tag ' + cls, text: label }),
                    r.error ? el('small', { class: 'gdw-err', text: r.error }) : null,
                    since !== null ? el('small', { text: shortTime(r.updated) + ' 시작, ' + since + '분 경과 · 끝나면 쌓인 변경부터 처리' }) : null,
                    statLine(r.stat)]),
                el('td', { text: String(r.items) }),
                el('td', { text: shortTime(r.last_event) || '-' }),
                el('td', {}, [el('button', {
                    type: 'button', class: 'gdw-btn gdw-btn-quiet', text: '처음부터',
                    title: '체크포인트와 추적 상태를 지우고 다시 시작',
                    onclick: function () {
                        if (confirm('[' + r.name + '] 체크포인트를 초기화할까요?\n이전 변경은 다시 감지하지 않고, 지금부터 새로 추적합니다.')) act('reset_root', { name: r.name });
                    }
                })])
            ]));
        });
    }

    function statLine(x) {
        if (!x) return el('small', { text: '아직 확인 전' });
        var t = shortTime(x.checked) + ' 확인 · ';
        if (x.note) t += x.note;
        else if (!x.raw) t += '변경 없음';
        else {
            t += 'Drive 변경 ' + x.raw + ' → 기록 ' + x.events;
            if (x.outside) t += ', 범위 밖 ' + x.outside;
            if (x.ext) t += ', 확장자 제외 ' + x.ext;
            if (x.same) t += ', 변화 없음 ' + x.same;
        }
        return el('small', { text: t + (x.elapsed ? ' (' + x.elapsed + '초)' : '') });
    }

    // ───────── 변경 기록 ─────────
    function loadEvents() {
        if (!alive()) return;
        rpc('events', { status: st.status, q: st.q, page: st.page, size: st.size }).then(function (d) {
            st.total = d.total;
            renderEvents(d.items || []);
        }).catch(function (e) { toast(e.message, true); });
    }
    function stage(ev, kind) {
        var list = (ev.result && ev.result[kind]) || [];
        if (kind === 'scans' && (ev.status === 'waiting' || ev.status === 'timeout')) return ev.status === 'waiting' ? 'wait' : 'fail';
        if (!list.length) {
            if (ev.status === 'pending' || ev.status === 'waiting') return 'wait';
            if (kind === 'scans' && ev.status === 'failed') return 'none';
            return 'none';
        }
        if (kind === 'vfs') return list.some(function (x) { return !x.ok; }) ? 'fail' : 'ok';
        if (list.some(function (x) { return x.ok === false; })) return 'fail';
        if (list.every(function (x) { return x.ok === null; })) return 'none';
        return 'ok';
    }
    function renderEvents(items) {
        var list = clear(bind('events'));
        if (!items.length) {
            list.appendChild(el('li', { class: 'gdw-empty', text: st.status || st.q ? '조건에 맞는 기록이 없습니다.' : '아직 감지된 변경이 없습니다.' }));
        }
        items.forEach(function (ev) {
            var path = ev.path || ev.removed_path;
            var moved = ev.removed_path && ev.removed_path !== ev.path;
            var statusText = STATUS[ev.status] || ev.status;
            if (ev.status === 'pending' && ev.attempts) statusText = '재시도 대기 (' + ev.attempts + '회 실패)';
            var check = el('input', { type: 'checkbox', 'aria-label': '선택', checked: !!st.picked[ev.id] });
            check.addEventListener('click', function (e) { e.stopPropagation(); if (check.checked) st.picked[ev.id] = 1; else delete st.picked[ev.id]; });
            var pipe = el('span', { class: 'gdw-pipe', title: '감지 → VFS 새로고침 → BookOasis 스캔' }, [
                el('i', { class: 'ok' }), el('i', { class: stage(ev, 'vfs') }), el('i', { class: stage(ev, 'scans') }),
                el('em', { text: statusText })
            ]);
            var row = el('div', { class: 'gdw-ev-row', role: 'button', tabindex: 0 }, [
                check,
                el('span', { class: 'gdw-ev-time', text: shortTime(ev.created) }),
                el('span', { class: 'gdw-tag', text: ACTION[ev.action] || ev.action }),
                el('div', { class: 'gdw-ev-path' }, [
                    el('div', { text: path + (ev.item_type === 'directory' ? '/' : ''), title: path }),
                    ev.action !== 'delete' && moved ? el('div', { class: 'from', text: ev.removed_path, title: ev.removed_path }) : null,
                    el('div', { class: 'root' }, [ev.root].concat(libraryTags(ev)))
                ]),
                pipe
            ]);
            var li = el('li', { class: 'gdw-ev' }, [row]);
            if (st.open[ev.id]) li.appendChild(detail(ev));
            function toggle() {
                if (st.open[ev.id]) { delete st.open[ev.id]; if (li.lastChild !== row) li.removeChild(li.lastChild); }
                else { st.open[ev.id] = 1; li.appendChild(detail(ev)); }
            }
            row.addEventListener('click', toggle);
            row.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } });
            list.appendChild(li);
        });
        var pages = Math.max(1, Math.ceil(st.total / st.size));
        bind('page').textContent = st.page + ' / ' + pages + ' (' + st.total + '건)';
        $('[data-act="prev"]').disabled = st.page <= 1;
        $('[data-act="next"]').disabled = st.page >= pages;
    }
    var DB_LABEL = { general: '일반', adult: '성인', audiobook: '오디오북', video: '영상' };
    function libraryName(label) {
        // 'general#36 네이버 웹툰(ZEEPS)' → '[일반] 네이버 웹툰(ZEEPS)'
        var m = /^(\w+)#\d+\s+(.*)$/.exec(label || '');
        return m ? '[' + (DB_LABEL[m[1]] || m[1]) + '] ' + m[2] : (label || '');
    }
    function libraryTags(ev) {
        var seen = {}, tags = [];
        ((ev.result && ev.result.scans) || []).forEach(function (x) {
            if (!x.library || seen[x.library]) return;
            seen[x.library] = 1;
            var cls = x.ok === true ? 'ok' : x.ok === false ? 'fail' : '';
            tags.push(el('span', { class: 'gdw-lib ' + cls, text: libraryName(x.library),
                title: (x.ok === true ? '반영된 보관함' : x.ok === false ? '스캔 실패한 보관함' : '보관함') + ' · 스캔한 폴더: ' + x.dir }));
        });
        return tags;
    }
    function detail(ev) {
        var r = ev.result || {};
        var box = el('div', { class: 'gdw-ev-detail' });
        box.appendChild(el('div', { text: '감지 ' + (ev.created || '').replace('T', ' ') + (ev.finished ? ' · 처리 ' + ev.finished.replace('T', ' ') : '') + ' · 시도 ' + ev.attempts + '회' }));
        if (ev.message) box.appendChild(el('div', { class: ev.status === 'waiting' ? 'msg wait' : 'msg', text: ev.message }));
        box.appendChild(el('h4', { text: 'VFS 새로고침' }));
        var v = el('ul');
        (r.vfs || []).forEach(function (x) {
            v.appendChild(el('li', { class: x.ok ? 'ok' : 'fail', text: (x.op === 'forget' ? '캐시 비우기 ' : '목록 새로고침 ') + x.path + '  (' + x.rc + ')' + (x.msg ? ' - ' + x.msg : '') }));
        });
        if (!(r.vfs || []).length) v.appendChild(el('li', { text: ev.status === 'pending' ? '처리 전' : '해당 VFS 규칙 없음' }));
        box.appendChild(v);
        box.appendChild(el('h4', { text: 'BookOasis 스캔' }));
        var s = el('ul');
        (r.scans || []).forEach(function (x) {
            var cls = x.ok === true ? 'ok' : x.ok === false ? 'fail' : '';
            s.appendChild(el('li', { class: cls, text: (x.library ? libraryName(x.library) + ' ← ' : '') + x.dir + (x.msg ? '  ' + x.msg : '') +
                (x.merged ? '  (같은 묶음의 상위 폴더 스캔에 합쳐짐)' : '') }));
        });
        if (!(r.scans || []).length) s.appendChild(el('li', { text: ev.status === 'pending' ? '처리 전' : ev.status === 'waiting' ? '파일이 마운트에 보이면 스캔' :
            ev.status === 'timeout' ? '파일이 보이지 않아 스캔하지 않음' : 'VFS 실패로 스캔 보류' }));
        box.appendChild(s);
        return box;
    }
    function pickedIds() { return Object.keys(st.picked).map(Number); }

    // ───────── 감시 설정 ─────────
    function loadSetup() {
        rpc('get_watch').then(function (d) {
            st.watch = d.watch;
            st.defaultIgnore = d.default_ignore || [];
            var s = d.settings || {};
            $$('[data-env]').forEach(function (i) {
                var k = i.getAttribute('data-env');
                if (k === 'auto_start') i.checked = !!s.auto_start;
                else if (k === 'clear_token') { i.checked = false; i.parentNode.hidden = !s.token_set; }
                else if (k === 'webhook_token') { i.value = ''; i.placeholder = s.token_set ? '저장됨 (바꿀 때만 입력)' : (s.env_token ? '비우면 .env의 값 사용' : '.env에도 없음, 입력 필요'); }
                else i.value = s[k] || '';
            });
            renderRoots(); renderVfs(); renderOpts(); loadVfsMap();
            bind('manual-box').open = st.watch.vfs.length > 0;
            if (d.legacy_removed) toast('예전 버전에서 추가한 VFS 규칙 ' + d.legacy_removed + '개를 정리했습니다. 이제 보관함 RC 주소로 자동 처리합니다.');
            checkRclone(true);
        }).catch(function (e) { toast(e.message, true); });
    }
    function envValues() {
        var env = {};
        $$('[data-env]').forEach(function (i) {
            var k = i.getAttribute('data-env');
            if (k === 'auto_start') env[k] = i.checked;
            else if (k === 'clear_token') { if (i.checked) env.webhook_token = ''; }
            else if (k === 'webhook_token') { if (i.value.trim()) env[k] = i.value.trim(); }
            else env[k] = i.value.trim();
        });
        return env;
    }
    function checkRclone(quiet) {
        var out = bind('rclone-out');
        rpc('remotes', { env: envValues() }).then(function (r) {
            var dl = document.getElementById('gdw-remotes') || document.body.appendChild(el('datalist', { id: 'gdw-remotes' }));
            clear(dl);
            (r.remotes || []).forEach(function (x) { if (x.type === 'drive') dl.appendChild(el('option', { value: x.name, label: x.type })); });
            clear(out);
            out.appendChild(el('p', { text: (r.version && r.version[0] ? r.version[0] + ' · ' : '') + '사용 중인 설정 파일: ' + (r.config_file || '알 수 없음') + (r.using_default ? ' (기본 위치)' : '') + (r.modified ? ' · 컨테이너에서 본 수정 시각 ' + r.modified : '') }));
            if (r.file_mount) out.appendChild(el('p', { class: 'bad', text: '이 파일은 도커에 파일 단위로 마운트되어 있습니다. 호스트에서 파일을 새로 써서 바꾸면(편집기 저장, rclone 토큰 갱신 등) 컨테이너는 옛 내용을 계속 봅니다. 디렉터리째 마운트하세요.' }));
            var drives = (r.remotes || []).filter(function (x) { return x.type === 'drive'; });
            var tbody = el('tbody');
            drives.forEach(function (x) {
                var exp = x.expiry ? new Date(x.expiry) : null;
                var expired = exp && !isNaN(exp) && exp < new Date();
                tbody.appendChild(el('tr', {}, [
                    el('td', { text: x.name }),
                    x.auth_mode === 'custom'
                        ? el('td', { class: 'dim', text: '파일 값 사용 안 함', title: 'rclone.conf에 적힌 만료 ' + String(x.expiry || '-').slice(0, 19).replace('T', ' ') +
                            ' — 이 리모트는 토큰을 인증 서버에서 받아 메모리에서만 쓰므로 파일의 시각은 바뀌지 않는 게 정상입니다. 실제 상태는 [토큰 가져오기 시험]으로 확인하세요.' })
                        : el('td', { class: expired ? 'bad' : '', text: x.expiry ? (expired ? '만료됨 ' : '') + String(x.expiry).slice(0, 19).replace('T', ' ') : '토큰 없음' }),
                    el('td', { text: (x.team_drive ? '공유 드라이브' : '내 드라이브') + ' · ' +
                        ({ memory: '메모리 갱신(파일 안 씀)', custom: '커스텀 인증 · rclone이 쓰는 토큰을 가져옴', rclone: 'rclone이 갱신' }[x.auth_mode] || '') }),
                    el('td', { class: /activity/.test(x.granted || '') ? '' : (/확인 불가|없음/.test(x.granted || '') ? '' : 'dim'),
                        text: x.granted || '', title: 'rclone.conf scope: ' + (x.scope_conf || '') }),
                    el('td', {}, [x.auth_mode === 'custom' ? el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet', text: '토큰 가져오기 시험',
                        title: '마운트 RC(config/get) 또는 지정한 rclone의 요청 헤더에서 토큰을 가져올 수 있는지 확인합니다.',
                        onclick: function (e) {
                            var b = e.target; b.disabled = true;
                            rpc('test_rc_token', { remote: x.name }).then(function (d) { toast(d.message); }).catch(function (err) { toast(err.message, true); })
                                .then(function () { b.disabled = false; });
                        } })
                      : x.auth_mode === 'memory' ? el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet', text: '갱신 시험', title: '플러그인이 메모리에서 직접 갱신합니다. rclone.conf에는 쓰지 않습니다.',
                        onclick: function (e) {
                            var b = e.target; b.disabled = true;
                            rpc('test_token', { remote: x.name }).then(function (d) { toast(d.message); }).catch(function (err) { toast(err.message, true); })
                                .then(function () { b.disabled = false; });
                        } })
                      : x.expiry ? el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet', text: '토큰 갱신',
                        onclick: function (e) {
                            var b = e.target; b.disabled = true; b.textContent = '갱신 중…';
                            rpc('refresh_token', { remote: x.name }).then(function (d) { toast(d.message); checkRclone(true); })
                                .catch(function (err) { toast(err.message, true); b.disabled = false; b.textContent = '토큰 갱신'; });
                        } }) : null])
                ]));
            });
            out.appendChild(el('table', {}, [el('thead', {}, [el('tr', {}, [el('th', { text: 'Drive 리모트' }), el('th', { text: '토큰 만료' }), el('th', { text: '비고' }), el('th', { text: '토큰의 실제 권한' }), el('th', { text: '' })])]), tbody]));
            if (!drives.length) out.appendChild(el('p', { text: '이 설정 파일에 Drive 리모트가 없습니다. rclone.conf 경로를 확인하세요.' }));
            drives.filter(function (x) { return x.auth_mode === 'custom' && /activity/.test(x.scope_conf || ''); }).forEach(function (x) {
                out.appendChild(el('p', { class: 'bad', text: x.name + ': 커스텀 인증 리모트의 scope에 drive.activity.readonly가 들어 있습니다. ' +
                    '이 리모트는 인증 서버가 정한 권한으로만 토큰을 받으므로 scope가 다르면 토큰을 받지 못합니다. rclone.conf에서 scope = drive로 되돌리세요 ' +
                    '(FF·호스트 마운트도 같은 파일을 씁니다).' }));
            });
            out.hidden = false;
        }).catch(function (e) {
            clear(out).appendChild(el('p', { class: 'bad', text: 'rclone 확인 실패: ' + e.message }));
            out.hidden = false;
            if (!quiet) toast(e.message, true);
        });
    }
    function field(label, input) { return el('label', {}, [label, input]); }
    function input(obj, key, attrs) {
        var i = el('input', Object.assign({ type: 'text', value: obj[key] === undefined ? '' : obj[key] }, attrs || {}));
        i.addEventListener('input', function () { obj[key] = i.type === 'number' ? Number(i.value) : i.value; });
        return i;
    }
    function checkbox(obj, key, label, def) {
        if (obj[key] === undefined) obj[key] = def;
        var i = el('input', { type: 'checkbox', checked: !!obj[key] });
        i.addEventListener('change', function () { obj[key] = i.checked; });
        return el('label', { class: 'check' }, [i, label]);
    }
    function renderRoots() {
        var box = clear(bind('root-rows'));
        st.watch.roots.forEach(function (r, idx) {
            var mode = el('select', {}, [el('option', { value: 'changes', text: 'Drive · Changes' }), el('option', { value: 'activity', text: 'Drive · Activity' }),
                el('option', { value: 'local', text: '로컬 폴더' })]);
            mode.value = r.mode || 'changes';
            mode.addEventListener('change', function () { r.mode = mode.value; renderRoots(); });
            var local = r.mode === 'local';
            var cells = [field('이름', input(r, 'name', { placeholder: local ? 'nas_books' : 'books' })), field('방식', mode)];
            if (local) {
                var detect = el('select', {}, [el('option', { value: 'auto', text: '자동' }), el('option', { value: 'inotify', text: '실시간 (로컬 디스크)' }),
                    el('option', { value: 'polling', text: '주기 비교 (NAS·마운트)' })]);
                detect.value = r.local_detect || 'auto';
                detect.addEventListener('change', function () { r.local_detect = detect.value; });
                if (!r.local_interval) r.local_interval = 300;
                cells.push(field('감지', detect), field('비교 주기(초)', input(r, 'local_interval', { type: 'number', min: 30 })),
                    field('감시할 폴더 (컨테이너 기준)', input(r, 'local_root', { placeholder: '/mnt/nas/책' })), el('span'));
            } else {
                cells.push(field('rclone 리모트', input(r, 'source_remote', { list: 'gdw-remotes', placeholder: 'zeeps_member' })),
                    field('Drive 폴더 ID', input(r, 'root_id', { placeholder: '1AbC…' })),
                    field('로컬 경로', input(r, 'local_root', { placeholder: '/mnt/gds/책' })),
                    checkbox(r, 'seed', '기존 파일 목록 수집', true));
            }
            cells.push(checkbox(r, 'enabled', '사용', true),
                el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet gdw-check-btn', text: '점검', title: '이 설정으로 실제 감시가 되는지 점검',
                    onclick: function (e) { checkRoot(r, e.target); } }),
                el('button', { type: 'button', class: 'del', title: '삭제', 'aria-label': '삭제', text: '×', onclick: function () { st.watch.roots.splice(idx, 1); renderRoots(); } }),
                el('div', { class: 'gdw-checks', 'data-check': idx, hidden: true }));
            box.appendChild(el('div', { class: 'gdw-row root' + (local ? ' is-local' : '') }, cells));
        });
        if (!st.watch.roots.length) box.appendChild(el('p', { class: 'gdw-help', text: '아직 없습니다. 폴더 추가를 눌러 감시할 Drive 폴더나 로컬 폴더를 등록하세요.' }));
    }
    function checkRoot(r, btn) {
        var box = btn.parentNode.querySelector('.gdw-checks');
        clear(box).appendChild(el('div', { class: 'gdw-muted', text: '점검 중…' }));
        box.hidden = false; btn.disabled = true;
        rpc('check_root', { root: r }).then(function (d) {
            clear(box);
            (d.checks || []).forEach(function (c) {
                box.appendChild(el('div', { class: 'gdw-checkline ' + (c.ok === true ? 'ok' : c.ok === false ? 'fail' : 'warn') }, [
                    el('b', { text: c.label }), el('span', { text: c.msg })
                ]));
            });
        }).catch(function (e) { clear(box).appendChild(el('div', { class: 'gdw-checkline fail' }, [el('b', { text: '점검' }), el('span', { text: e.message })])); })
          .then(function () { btn.disabled = false; });
    }
    function renderVfs() {
        var box = clear(bind('vfs-rows'));
        st.watch.vfs.forEach(function (r, idx) {
            box.appendChild(el('div', { class: 'gdw-row vfs' }, [
                field('마운트 루트 (컨테이너 기준)', input(r, 'local', { placeholder: '/mnt/gds2' })),
                field('RC 주소', input(r, 'rc', { placeholder: 'http://192.168.0.90:5275' })),
                field('마운트 안 하위 경로', input(r, 'remote', { placeholder: '비우면 마운트 루트' })),
                field('fs (선택)', input(r, 'fs', { placeholder: 'union_gds:' })),
                el('button', { type: 'button', class: 'del', title: '삭제', 'aria-label': '삭제', text: '×', onclick: function () { st.watch.vfs.splice(idx, 1); renderVfs(); } })
            ]));
        });
        if (!st.watch.vfs.length) box.appendChild(el('p', { class: 'gdw-help', text: '직접 지정한 규칙이 없습니다. 자동 감지만 사용합니다.' }));
    }
    function renderOpts() {
        $$('[data-opt]').forEach(function (i) {
            var key = i.getAttribute('data-opt');
            if (i.type === 'checkbox') {
                if (st.watch[key] === undefined) st.watch[key] = key.indexOf('notify_') === 0;
                i.checked = !!st.watch[key];
                i.onchange = function () { st.watch[key] = i.checked; };
                return;
            }
            if (i.tagName === 'TEXTAREA') {
                var v = st.watch[key];
                i.value = Array.isArray(v) ? v.join('\n') : (v || '');
                i.oninput = function () { st.watch[key] = i.value.split('\n'); };
                return;
            }
            i.value = st.watch[key] === undefined ? '' : st.watch[key];
            i.oninput = function () { st.watch[key] = i.type === 'number' ? Number(i.value) : i.value; };
        });
    }
    function loadVfsMap() {
        rpc('vfs_map').then(function (d) {
            var tbody = clear(bind('vfs-map'));
            var type = { general: '일반', adult: '성인', audiobook: '오디오북', video: '영상' };
            var withRc = 0, found = 0;
            (d.items || []).forEach(function (x) {
                if (x.rc) withRc++;
                if (x.remote !== null) found++;
                var mapped = !x.rc ? el('td', { class: 'none', text: 'RC 없음 (새로고침 안 함)' })
                    : x.remote === null ? el('td', { class: 'pending', text: '처음 변경 때 감지' })
                    : el('td', { text: (x.fs || '') + (x.remote || '(마운트 루트)'), title: '감지 ' + x.detected });
                tbody.appendChild(el('tr', {}, [
                    el('td', { text: '[' + (type[x.db_type] || x.db_type) + '] ' + x.name }),
                    el('td', { text: x.root }),
                    el('td', { text: x.rc || '-' }),
                    mapped,
                    el('td', {}, [x.rc ? el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet', text: x.remote === null ? '감지' : '다시 감지',
                        onclick: function () { act('detect_vfs', { root: x.root }, false).then(loadVfsMap); } }) : null])
                ]));
            });
            bind('vfs-summary').textContent = '보관함 경로 ' + (d.items || []).length + '개 중 RC 설정 ' + withRc + '개, 감지 완료 ' + found + '개';
        }).catch(function (e) { toast(e.message, true); });
    }

    // ───────── 이벤트 연결 ─────────
    app.addEventListener('click', function (e) {
        var btn = e.target.closest('[data-act]');
        if (btn) {
            var a = btn.getAttribute('data-act');
            if (a === 'toggle') act(st.alive ? 'stop' : 'start');
            else if (a === 'restart') act('restart');
            else if (a === 'poll_now') act('poll_now');
            else if (a === 'retry_selected') { if (!pickedIds().length) return toast('재시도할 기록을 선택하세요.', true); act('retry', { ids: pickedIds() }); st.picked = {}; }
            else if (a === 'retry_failed') act('retry', { all_failed: true });
            else if (a === 'delete_selected') { if (!pickedIds().length) return toast('삭제할 기록을 선택하세요.', true); if (confirm(pickedIds().length + '건을 삭제할까요?')) { act('delete', { ids: pickedIds() }); st.picked = {}; } }
            else if (a === 'clear_done') { if (confirm('반영됨·보관함 밖 기록을 모두 지울까요?')) act('delete', { clear: 'done' }); }
            else if (a === 'prev') { st.page = Math.max(1, st.page - 1); loadEvents(); }
            else if (a === 'next') { st.page += 1; loadEvents(); }
            else if (a === 'add_root') { st.watch.roots.push({ name: '', mode: 'changes', source_remote: '', root_id: '', local_root: '', seed: true, enabled: true }); renderRoots(); }
            else if (a === 'add_vfs') { st.watch.vfs.push({ local: '', rc: '', remote: '', fs: '' }); renderVfs(); }
            else if (a === 'save') {
                bind('save-msg').textContent = '저장 중…';
                rpc('save_watch', { watch: st.watch }).then(function (d) { bind('save-msg').textContent = d.message; loadStatus(); })
                    .catch(function (err) { bind('save-msg').textContent = ''; toast(err.message, true); });
            }
            else if (a === 'preview') {
                var out = bind('preview-out');
                rpc('preview', { path: bind('preview-path').value }).then(function (d) {
                    var lines = ['감시 루트: ' + (d.roots.length ? d.roots.join(', ') : '없음 (이 경로의 변경은 감지되지 않음)')];
                    lines.push('VFS: ' + (d.vfs.length ? d.vfs.map(function (v) { return v.rc + (v.fs ? ' fs=' + v.fs : '') + '  dir="' + v.dir + '"'; }).join('\n     ') : '해당 규칙 없음'));
                    lines.push('스캔: ' + (d.library ? d.library.db_type + '#' + d.library.id + ' ' + d.library.name + '  path="' + d.library.path + '"' : '해당 보관함 없음 (건너뜀)'));
                    out.textContent = lines.join('\n'); out.hidden = false;
                }).catch(function (err) { out.textContent = err.message; out.hidden = false; });
            }
            else if (a === 'log') loadLog();
            else if (a === 'check_rclone') checkRclone(false);
            else if (a === 'manual_create' || a === 'manual_delete') {
                var mp = bind('manual-path').value.trim();
                if (!mp) return toast('반영할 경로를 입력하세요.', true);
                act('manual', { path: mp, action: a === 'manual_delete' ? 'delete' : 'create' }).then(function () { bind('manual-path').value = ''; });
            }
            else if (a === 'default_patterns') { st.watch.ignore_patterns = (st.defaultIgnore || []).slice(); renderOpts(); }
            else if (a === 'test_discord') { rpc('test_discord', { url: st.watch.discord_webhook || '' }).then(function (d) { toast(d.message); }).catch(function (err) { toast(err.message, true); }); }
            else if (a === 'check_token') {
                rpc('check_token', { env: envValues() }).then(function (d) { bind('env-msg').textContent = (d.ok ? '✓ ' : '✗ ') + d.message; })
                    .catch(function (err) { toast(err.message, true); });
            }
            else if (a === 'detect_all') { btn.disabled = true; toast('감지 중입니다. 보관함이 많으면 시간이 걸립니다.'); act('detect_vfs', {}, false).then(function () { btn.disabled = false; loadVfsMap(); }); }
            else if (a === 'save_env') {
                bind('env-msg').textContent = '저장 중…';
                rpc('save_env', { env: envValues() }).then(function (d) { bind('env-msg').textContent = d.message; loadSetup(); loadStatus(); })
                    .catch(function (err) { bind('env-msg').textContent = ''; toast(err.message, true); });
            }
            return;
        }
        var seg = e.target.closest('[data-filter]');
        if (seg) {
            var f = seg.getAttribute('data-filter');
            st.status = st.status === f ? '' : f;
            bind('f-status').value = st.status; st.page = 1;
            switchTab('events'); loadEvents(); loadStatus();
            return;
        }
        var tab = e.target.closest('[data-tab]');
        if (tab) switchTab(tab.getAttribute('data-tab'));
    });
    bind('f-status').addEventListener('change', function () { st.status = this.value; st.page = 1; loadEvents(); loadStatus(); });
    var qTimer;
    bind('f-q').addEventListener('input', function () { var v = this.value; clearTimeout(qTimer); qTimer = setTimeout(function () { st.q = v.trim(); st.page = 1; loadEvents(); }, 300); });

    function switchTab(name) {
        st.tab = name;
        $$('[data-tab]').forEach(function (b) { b.classList.toggle('is-on', b.getAttribute('data-tab') === name); b.setAttribute('aria-selected', b.getAttribute('data-tab') === name); });
        $$('[data-pane]').forEach(function (p) { p.hidden = p.getAttribute('data-pane') !== name; });
        if (name === 'events') loadEvents();
        else if (name === 'setup') loadSetup();
        else if (name === 'log') loadLog();
    }
    function loadLog() {
        rpc('log', { lines: 300 }).then(function (d) { var pre = bind('log'); pre.textContent = d.text; pre.scrollTop = pre.scrollHeight; })
            .catch(function (e) { toast(e.message, true); });
    }

    // 주기 갱신: 화면이 보이고 탭이 살아 있을 때만
    function tick(fn) {
        return function () {
            if (!alive()) { window.__gdwTimers.forEach(clearInterval); return; }
            if (document.visibilityState === 'visible') fn();
        };
    }
    window.__gdwTimers.push(setInterval(tick(loadStatus), 5000));
    window.__gdwTimers.push(setInterval(tick(function () {
        if (st.tab === 'events' && st.page === 1) loadEvents();
        else if (st.tab === 'log') loadLog();
    }), 10000));

    loadStatus();
    loadEvents();
})();
