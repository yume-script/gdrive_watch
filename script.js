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
    var STATUS = { pending: '대기', waiting: '잠시 대기', done: '반영됨', skipped: '보관함 밖', failed: '실패', timeout: '시간 초과' };
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
        var ver = bind('version');
        if (d.version) { ver.textContent = 'v' + d.version; ver.hidden = false; } else { ver.hidden = true; }
        bind('roots-badge').textContent = (d.roots || []).length + '개';

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
                el('td', { text: (r.fallback === 'userfeed' || r.fallback === 'drivepoll') ? 'Drive · 폴더 비교 (변경 목록 사용 불가로 자동 전환)' : r.mode === 'drivepoll' ? 'Drive · 폴더 비교' : r.mode === 'activity' ? (r.fallback ? 'Drive · Changes (Activity 권한 없어 자동 전환)' : 'Drive · Activity') : r.mode === 'local' ? '로컬' + (r.local_detect === 'polling' ? ' · 주기' : r.local_detect === 'inotify' ? ' · 실시간' : '') : 'Drive · Changes' }),
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
            if (ev.status === 'waiting') statusText = /전체 스캔/.test(ev.message || '') ? '전체 스캔 회피' : '파일 확인 대기';
            if ((ev.status === 'pending' || ev.status === 'waiting') && ev.ready_at) {
                var left = Math.round(ev.ready_at - Date.now() / 1000);
                statusText += left > 0 ? ' · ' + (left >= 60 ? Math.ceil(left / 60) + '분 후' : left + '초 후') : ' · 곧 처리';
            }
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
                el('option', { value: 'drivepoll', text: 'Drive · 폴더 비교' }), el('option', { value: 'local', text: '로컬 폴더' })]);
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
                    field('로컬 경로', input(r, 'local_root', { placeholder: '/mnt/gds/책' })));
                if (r.mode === 'drivepoll') {
                    if (!r.drive_interval) r.drive_interval = 600;
                    cells.push(field('비교 주기(초)', input(r, 'drive_interval', { type: 'number', min: 120 })));
                } else {
                    cells.push(checkbox(r, 'seed', '기존 파일 목록 수집', true));
                }
            }
            cells.push(checkbox(r, 'enabled', '사용', true),
                el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet gdw-check-btn', text: '점검', title: '이 설정으로 실제 감시가 되는지 점검',
                    onclick: function (e) { checkRoot(r, e.target); } }),
                (!local && r.mode === 'drivepoll') ? el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet gdw-check-btn', text: '빠른 확인 시험',
                    title: 'Drive 검색으로 새로 생긴 파일을 바로 찾을 수 있는지 시험합니다 (시작 → 파일 업로드 → 결과 확인)',
                    onclick: function (e) { quickProbe(r, e.target); } }) : null,
                (local || r.mode === 'drivepoll') ? null : el('button', { type: 'button', class: 'gdw-btn gdw-btn-quiet gdw-check-btn', text: '변경 목록 시험',
                    title: '지금부터 Drive 변경 목록에 무엇이 들어오는지 직접 확인합니다 (시작 → 파일 업로드 → 결과 확인)',
                    onclick: function (e) { probeRoot(r, e.target); } }),
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
    function probeRoot(r, btn) {
        var box = btn.parentNode.querySelector('.gdw-checks');
        box.hidden = false;
        var line = function (cls, label, text) { return el('div', { class: 'gdw-checkline ' + cls }, [el('b', { text: label }), el('span', { text: text })]); };
        if (btn.getAttribute('data-probing') !== '1') {
            btn.disabled = true;
            rpc('feed_probe_start', { root: r }).then(function (d) {
                clear(box).appendChild(line('warn', '변경 목록 시험', d.message));
                btn.textContent = '결과 확인'; btn.setAttribute('data-probing', '1');
            }).catch(function (e) { clear(box).appendChild(line('fail', '변경 목록 시험', e.message)); })
              .then(function () { btn.disabled = false; });
            return;
        }
        btn.disabled = true;
        rpc('feed_probe_check', { root: r }).then(function (d) {
            clear(box).appendChild(line(/정상/.test(d.verdict) ? 'ok' : 'fail', '결과', d.verdict));
            box.appendChild(line('', '변경 목록', d.feed + ' · ' + d.total + '건 (최근 30건)'));
            (d.rows || []).forEach(function (x) {
                box.appendChild(line(x.inside ? 'ok' : '', x.inside ? '감시 폴더 안' : (x.inside === false ? '범위 밖' : '확인 불가'),
                    (x.removed ? '[삭제] ' : '') + x.name));
            });
            btn.textContent = '변경 목록 시험'; btn.removeAttribute('data-probing');
        }).catch(function (e) { box.appendChild(line('fail', '결과', e.message)); })
          .then(function () { btn.disabled = false; });
    }
    function quickProbe(r, btn) {
        var box = btn.parentNode.querySelector('.gdw-checks');
        box.hidden = false;
        var line = function (cls, label, text) { return el('div', { class: 'gdw-checkline ' + cls }, [el('b', { text: label }), el('span', { text: text })]); };
        btn.disabled = true;
        if (btn.getAttribute('data-probing') !== '1') {
            rpc('quick_probe_start', { root: r }).then(function (d) {
                clear(box).appendChild(line('warn', '빠른 확인 시험', d.message));
                btn.textContent = '결과 확인'; btn.setAttribute('data-probing', '1');
            }).catch(function (e) { clear(box).appendChild(line('fail', '빠른 확인 시험', e.message)); })
              .then(function () { btn.disabled = false; });
            return;
        }
        rpc('quick_probe_check', { root: r }).then(function (d) {
            clear(box).appendChild(line(d.ok ? 'ok' : 'fail', '결과', d.verdict));
            (d.results || []).forEach(function (x) {
                if (x.error) { box.appendChild(line('fail', '검색 ' + x.corpora, x.error)); return; }
                box.appendChild(line(x.hit_count ? 'ok' : '', '검색 ' + x.corpora,
                    '결과 ' + x.total + (x.more ? '건 이상' : '건') + ' · ' + x.seconds + '초 · 감시 폴더 안 ' + x.hit_count + '건' +
                    (x.hits && x.hits.length ? ' (' + x.hits.join(', ') + ')' : '')));
            });
            btn.textContent = '빠른 확인 시험'; btn.removeAttribute('data-probing');
        }).catch(function (e) { box.appendChild(line('fail', '결과', e.message)); })
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
        else if (name === 'schedule' && window.__gdwScheduleLoad) window.__gdwScheduleLoad();
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

// ─────────────────────────── 스캔 일정 (구 scan_scheduler 통합) ───────────────────────────
// scan_scheduler v1.2.0의 타임테이블·겹침 표시·스케줄 도우미를 옮겨 왔다.
// 데이터 조회/저장만 gdrive_watch RPC(schedules / update_cron)로 바꾸고,
// 감시 중인 보관함에 '실시간 반영 중' 표시를 더했다.
(function () {
  'use strict';
  var scopeEl = (typeof container !== 'undefined' && container && container.querySelector) ? container : document;
  var pane = scopeEl.querySelector('#gdw-app [data-pane="schedule"]');
  if (!pane) return;
  var PID = (typeof pluginId !== 'undefined' && pluginId) || 'gdrive_watch';
  function rpc(action, context) {
    return fetch('/api/media/context-menu/book/plugins/action', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'general', plugin_id: PID, action_id: action, context: context || {} })
    }).then(function (res) { return res.json().catch(function () { return { success: false, error: 'HTTP ' + res.status }; }); })
      .then(function (data) { if (!data || !data.success) throw new Error((data && (data.error || data.message)) || '요청 실패'); return data; });
  }
  (function (container, pluginId) {

  const LOG_PREFIX = '[scan_scheduler]';
  console.log(LOG_PREFIX, '0/3 Fullpage Timetable UI loaded.');

  let allItems = [];
  let currentEditItem = null; // 지금 편집 패널에서 다루고 있는 item (allItems의 원소 참조)
  let helperListenersBound = false;
  let viewMode = 'grid'; // 'grid' (요일×시간) | 'timeline' (라이브러리별)

  const SCOPE_COLORS = {
    general: '#3b82f6',
    adult: '#ec4899',
    audiobook: '#22c55e',
    video: '#f97316',
  };

  // 그리드 뷰의 요일 컬럼 순서(월~일). dow는 cron 표준(0=일요일)의 값.
  const GRID_DAYS = [
    { label: '월', dow: 1 },
    { label: '화', dow: 2 },
    { label: '수', dow: 3 },
    { label: '목', dow: 4 },
    { label: '금', dow: 5 },
    { label: '토', dow: 6 },
    { label: '일', dow: 0 },
  ];

  // ------------------------------------------------------------------
  // cron 파싱 (분/시 필드만 사용, 표준 5필드 cron 가정: 분 시 일 월 요일)
  // ------------------------------------------------------------------
  function parseCronField(field, min, max) {
    if (!field || field === '*') {
      const arr = [];
      for (let i = min; i <= max; i += 1) arr.push(i);
      return arr;
    }
    const result = [];
    field.split(',').forEach((part) => {
      let step = 1;
      let rangePart = part;
      if (part.includes('/')) {
        const [r, s] = part.split('/');
        rangePart = r;
        step = parseInt(s, 10) || 1;
      }
      let start = min;
      let end = max;
      if (rangePart !== '*') {
        if (rangePart.includes('-')) {
          const [s, e] = rangePart.split('-').map(Number);
          if (!Number.isNaN(s)) start = s;
          if (!Number.isNaN(e)) end = e;
        } else {
          const v = parseInt(rangePart, 10);
          if (!Number.isNaN(v)) {
            start = v;
            end = v;
          }
        }
      }
      for (let i = start; i <= end; i += step) result.push(i);
    });
    return Array.from(new Set(result)).sort((a, b) => a - b);
  }

  // 요일별 색상 (겹침 여부와 무관하게 항상 이 색으로 표시, 겹치면 빨간 테두리 추가)
  const DOW_COLORS = {
    null: '#94a3b8', // 매일(요일 필드가 *) - 슬레이트
    0: '#ef4444', // 일요일 - 빨강
    1: '#f97316', // 월요일 - 주황
    2: '#eab308', // 화요일 - 노랑
    3: '#22c55e', // 수요일 - 초록
    4: '#06b6d4', // 목요일 - 청록
    5: '#3b82f6', // 금요일 - 파랑
    6: '#a855f7', // 토요일 - 보라
  };
  const DOW_LABELS = ['일', '월', '화', '수', '목', '금', '토'];

  // cron 문자열 -> [{hour, minute, dow, dom}, ...].
  // - 요일(dow) 필드가 지정되면 요일 발생(dow=0~6, dom=null)으로 전개 (매주 도우미).
  // - 요일 필드가 '*'이고 일(dom) 필드가 지정되면 "매월 N일" 발생(dow=null, dom=1~31)으로
  //   전개 (매월 도우미). 표준 cron은 dom/dow가 둘 다 지정되면 OR로 해석하지만, 이 도우미는
  //   둘을 동시에 생성하지 않으므로 요일 지정을 우선시한다.
  // - 둘 다 '*'이면 "매일" 하나로 취급(dow=null, dom=null).
  // 파싱 실패/빈 값이면 빈 배열.
  function cronToTimes(cronStr) {
    if (!cronStr || typeof cronStr !== 'string') return [];
    const fields = cronStr.trim().split(/\s+/);
    if (fields.length < 2) return [];
    try {
      const minutes = parseCronField(fields[0], 0, 59);
      const hours = parseCronField(fields[1], 0, 23);
      const domField = fields.length >= 3 ? fields[2] : '*';
      const dowField = fields.length >= 5 ? fields[4] : '*';

      const hasDow = !(!dowField || dowField === '*');
      const hasDom = !hasDow && !(!domField || domField === '*');

      const dowValues = hasDow ? parseCronField(dowField, 0, 6) : [null];
      const domValues = hasDom ? parseCronField(domField, 1, 31) : [null];

      const times = [];
      hours.forEach((h) => {
        minutes.forEach((m) => {
          if (hasDow) {
            dowValues.forEach((d) => times.push({ hour: h, minute: m, dow: d, dom: null }));
          } else if (hasDom) {
            domValues.forEach((dm) => times.push({ hour: h, minute: m, dow: null, dom: dm }));
          } else {
            times.push({ hour: h, minute: m, dow: null, dom: null });
          }
        });
      });
      return times;
    } catch (e) {
      console.warn(LOG_PREFIX, 'cron 파싱 실패:', cronStr, e);
      return [];
    }
  }

  function pad2(n) {
    return String(n).padStart(2, '0');
  }

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function itemKey(item) {
    return `${item.scope}:${item.id}`;
  }

  // 편집 중이면 아직 저장 안 된 미리보기 값을, 아니면 실제 저장된 값을 반환
  function effectiveCron(item) {
    return item._pendingCron != null ? item._pendingCron : item.cron_schedule;
  }

  // 두 발생(occurrence, {dow, dom})이 실제로 같은 날 겹치는지 판정.
  // - 매일(dow=null, dom=null) 발생은 항상 겹침 대상(그 시각엔 매일 실행되므로).
  // - 요일 지정 발생끼리는 요일이 같아야 겹침.
  // - 매월 특정 일(dom) 지정 발생끼리는 일자가 같아야 겹침.
  // - 요일 지정 vs 매월 특정 일 지정은 그 일자가 매달 다른 요일에 걸리므로 정확히
  //   판정할 수 없어 보수적으로 겹침 아님으로 처리한다(오탐 방지).
  function sameDay(a, b) {
    const aDaily = a.dow === null && a.dom === null;
    const bDaily = b.dow === null && b.dom === null;
    if (aDaily || bDaily) return true;
    if (a.dow !== null && b.dow !== null) return a.dow === b.dow;
    if (a.dom !== null && b.dom !== null) return a.dom === b.dom;
    return false;
  }

  // ------------------------------------------------------------------
  // 겹침 계산: scope 구분 없이 전체(같은 서버/스토리지 자원을 공유한다고
  // 가정)에서 같은 요일 + 같은 hour:minute에 2개 이상 라이브러리가 몰리면
  // "겹침"으로 표시. 요일이 다르면(예: 일요일 03:00 vs 월요일 03:00) 시:분이
  // 같아도 겹침으로 보지 않는다. 편집 중인 항목은 미리보기(pending) 값 기준.
  // 겹침 결과는 각 occurrence 객체(t)에 t.isOverlap으로 직접 표시해둔다.
  // ------------------------------------------------------------------
  function computeOverlapMap(items) {
    const occurrences = []; // [{item, t}, ...] 전체 발생 목록 (너무 촘촘한 cron은 제외)
    items.forEach((item) => {
      const times = cronToTimes(effectiveCron(item));
      item._times = times;
      if (times.length > 96) return; // 매우 잦은 주기는 겹침 판정에서 제외
      times.forEach((t) => {
        t.isOverlap = false;
        occurrences.push({ item, t });
      });
    });

    let overlapCount = 0;
    for (let i = 0; i < occurrences.length; i += 1) {
      for (let j = i + 1; j < occurrences.length; j += 1) {
        const a = occurrences[i];
        const b = occurrences[j];
        if (a.item === b.item) continue; // 같은 라이브러리 내부 발생끼리는 비교 안 함
        if (a.t.hour !== b.t.hour || a.t.minute !== b.t.minute) continue;
        if (!sameDay(a.t, b.t)) continue;
        if (!a.t.isOverlap) overlapCount += 1;
        if (!b.t.isOverlap) overlapCount += 1;
        a.t.isOverlap = true;
        b.t.isOverlap = true;
      }
    }
    return overlapCount;
  }

  function renderAxis() {
    const row = el('div', 'rm-axis-row');
    for (let h = 0; h < 24; h += 1) {
      row.appendChild(el('span', 'rm-axis-cell', h % 3 === 0 ? `${pad2(h)}시` : ''));
    }
    return row;
  }

  function renderTimeline(item) {
    const timeline = el('div', 'rm-timeline');
    const times = item._times || [];
    const cronStr = effectiveCron(item);

    if (times.length === 0) {
      const empty = el('span', 'rm-no-schedule', 'cron 없음/파싱불가');
      empty.style.position = 'absolute';
      empty.style.left = '4px';
      empty.style.top = '50%';
      empty.style.transform = 'translateY(-50%)';
      timeline.appendChild(empty);
      return timeline;
    }

    if (times.length > 96) {
      const bar = el('div');
      bar.style.position = 'absolute';
      bar.style.left = '0';
      bar.style.right = '0';
      bar.style.top = '50%';
      bar.style.height = '6px';
      bar.style.transform = 'translateY(-50%)';
      bar.style.borderRadius = '3px';
      bar.style.background = 'rgba(59, 130, 246, 0.55)';
      bar.title = `${cronStr} (매우 잦은 주기)`;
      timeline.appendChild(bar);
      return timeline;
    }

    times.forEach((t) => {
      const isMonthly = t.dow === null && t.dom !== null;
      const isWeekly = t.dow !== null;
      const pct = ((t.hour * 60 + t.minute) / 1440) * 100;
      const dayText = isMonthly ? `매월 ${t.dom}일` : isWeekly ? `${DOW_LABELS[t.dow]}요일` : '매일';
      const titleText = `${item.name} · ${dayText} ${pad2(t.hour)}:${pad2(t.minute)}${
        t.isOverlap ? ' (같은 시점에 다른 라이브러리와 겹침)' : ''
      }`;

      if (isMonthly) {
        // 매월 마커는 원형 마커와 구분되도록 사각형으로 표시하고, 바로 옆에
        // "N일" 텍스트를 함께 붙여서 요일 그리드에 없는 날짜 정보를 바로 알 수 있게 한다.
        const wrap = el('div', 'rm-monthly-marker-wrap' + (t.isOverlap ? ' rm-overlap' : ''));
        wrap.style.left = `${pct}%`;
        wrap.title = titleText;

        const dot = el('span', 'rm-marker rm-marker-monthly');
        dot.style.position = 'static';
        dot.style.top = 'auto';
        dot.style.transform = 'none';
        dot.style.backgroundColor = '#6366f1';
        wrap.appendChild(dot);

        wrap.appendChild(el('span', 'rm-monthly-marker-label', `${t.dom}일`));

        timeline.appendChild(wrap);
        return;
      }

      if (isWeekly) {
        // 매주 마커도 매월과 동일한 방식으로 요일 한 글자를 색상 텍스트로 옆에 붙인다.
        const color = DOW_COLORS[t.dow];
        const wrap = el('div', 'rm-weekly-marker-wrap' + (t.isOverlap ? ' rm-overlap' : ''));
        wrap.style.left = `${pct}%`;
        wrap.title = titleText;

        const dot = el('span', 'rm-marker');
        dot.style.position = 'static';
        dot.style.top = 'auto';
        dot.style.transform = 'none';
        dot.style.backgroundColor = color;
        wrap.appendChild(dot);

        const label = el('span', 'rm-weekly-marker-label', DOW_LABELS[t.dow]);
        label.style.color = color;
        wrap.appendChild(label);

        timeline.appendChild(wrap);
        return;
      }

      // 매일(dow=null, dom=null) 발생은 요일/날짜 텍스트 없이 기존처럼 점으로만 표시.
      const marker = el('div', 'rm-marker' + (t.isOverlap ? ' rm-overlap' : ''));
      if (times.length > 12) marker.classList.add('rm-many');
      marker.style.backgroundColor = DOW_COLORS[t.dow];
      marker.style.left = `${pct}%`;
      marker.title = titleText;
      timeline.appendChild(marker);
    });

    return timeline;
  }

  function renderLibRow(item) {
    const isEditing = currentEditItem && itemKey(currentEditItem) === itemKey(item);
    const row = el('div', 'rm-lib-row' + (isEditing ? ' rm-editing' : ''));

    const label = el('div', 'rm-lib-label');
    const nameSpan = el('span', null, item.name);
    label.appendChild(nameSpan);

    const icons = el('span', 'rm-lib-icons');
    if (item.is_remote) {
      const cloud = el('i', 'fa-solid fa-cloud');
      cloud.title = item.rclone_rc_url
        ? `원격(rclone) 마운트: ${item.rclone_rc_url}`
        : '원격(rclone) 마운트';
      icons.appendChild(cloud);
    }
    if (item.vfs_refresh_before_scan) {
      const rotate = el('i', 'fa-solid fa-arrows-rotate');
      rotate.title = '스캔 전 VFS refresh 수행';
      icons.appendChild(rotate);
    }
    if (item.watched) {
      const bolt = el('i', 'fa-solid fa-bolt rm-live-icon');
      bolt.title = `실시간 반영 중 (감시 폴더: ${(item.watch_roots || []).join(', ')})` +
        (item.frequent ? ' · 전체 스캔은 하루 1회 정도로 줄여도 됩니다' : '');
      icons.appendChild(bolt);
    }
    const editBtn = el('button', 'rm-edit-btn');
    editBtn.type = 'button';
    editBtn.title = '스케줄 편집';
    editBtn.innerHTML = '<i class="fa-solid fa-pen"></i>';
    editBtn.addEventListener('click', () => openEditPanel(item));
    icons.appendChild(editBtn);
    label.appendChild(icons);

    row.appendChild(label);
    row.appendChild(renderTimeline(item));
    return row;
  }

  function renderTimelineInto(container_, items) {
    const scopeOrder = [];
    const byScope = new Map();
    items.forEach((item) => {
      if (!byScope.has(item.scope)) {
        byScope.set(item.scope, []);
        scopeOrder.push(item);
      }
      byScope.get(item.scope).push(item);
    });

    scopeOrder.forEach((firstItem) => {
      const scopeKey = firstItem.scope;
      const scopeItems = byScope.get(scopeKey);
      const block = el('div', 'rm-scope-block');

      const header = el(
        'div',
        'rm-scope-header',
        `${firstItem.scope_label || scopeKey} (${scopeItems.length}개 라이브러리)`
      );
      block.appendChild(header);

      if (scopeItems.length === 0) {
        block.appendChild(el('div', 'rm-scope-empty', '등록된 라이브러리가 없습니다.'));
      } else {
        block.appendChild(renderAxis());
        scopeItems.forEach((item) => {
          block.appendChild(renderLibRow(item));
        });
      }

      container_.appendChild(block);
    });

    if (items.length === 0) {
      container_.appendChild(el('div', 'rm-scope-empty', '표시할 라이브러리가 없습니다.'));
    }
  }

  function hexToRgba(hex, alpha) {
    const h = (hex || '#64748b').replace('#', '');
    const num = parseInt(h, 16);
    const r = (num >> 16) & 255;
    const g = (num >> 8) & 255;
    const b = num & 255;
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  // 요일(월~일) x 24시간 매트릭스 뷰. item._times(computeOverlapMap가 이미
  // 채워둔 값, dow/hour/minute 포함)를 그대로 재사용해 셀별로 묶는다.
  // "매일"(dow=null) 스케줄은 7개 요일 칸 모두에 나타난다.
  function renderGridTable(items) {
    const wrapper = el('div', 'rm-grid-wrapper');

    if (items.length === 0) {
      wrapper.appendChild(el('div', 'rm-scope-empty', '표시할 라이브러리가 없습니다.'));
      return wrapper;
    }

    // "dow:slot" -> [{item, isMonthly, t}, ...] (같은 항목 중복 없이). slot = hour*2 + (0|1),
    // 30분 단위(00분/30분)로 쪼갠 인덱스(0~47).
    // 매월 특정 일(dom) 발생은 요일 정보가 없으므로("dow" 필드가 없음) "매일"과 동일하게
    // 7개 요일 칸 모두에 채워 넣는다 - 실제 실행 요일이 아니라 "이 시각에 실행되는
    // 일정이 있다"는 것을 전체 그리드에서 한눈에 보기 위함이며, 착각하지 않도록
    // 칩 라벨 앞에 "[월별]"을 붙여 구분한다.
    const grid = {};
    items.forEach((item) => {
      const seenKeys = new Set();
      (item._times || []).forEach((t) => {
        const slot = t.hour * 2 + (t.minute >= 30 ? 1 : 0);
        const isMonthly = t.dow === null && t.dom !== null;
        const days = t.dow === null ? [0, 1, 2, 3, 4, 5, 6] : [t.dow];
        days.forEach((d) => {
          const key = `${d}:${slot}`;
          if (seenKeys.has(key)) return;
          seenKeys.add(key);
          if (!grid[key]) grid[key] = [];
          grid[key].push({ item, isMonthly, t });
        });
      });
    });

    const table = el('table', 'rm-grid-table');

    const thead = el('thead');
    const headRow = el('tr');
    headRow.appendChild(el('th', 'rm-grid-corner', '시간'));
    GRID_DAYS.forEach((d) => headRow.appendChild(el('th', 'rm-grid-daycol', `${d.label}요일`)));
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = el('tbody');
    for (let slot = 0; slot < 48; slot += 1) {
      const h = Math.floor(slot / 2);
      const m = slot % 2 === 0 ? '00' : '30';
      const row = el('tr', 'rm-grid-row' + (m === '00' ? ' rm-grid-hour-start' : ''));
      row.appendChild(el('td', 'rm-grid-hourcol', `${pad2(h)}:${m}`));
      GRID_DAYS.forEach((d) => {
        const key = `${d.dow}:${slot}`;
        const cellItems = grid[key] || [];
        const td = el('td', 'rm-grid-cell' + (cellItems.length > 1 ? ' rm-grid-overlap' : ''));
        cellItems.forEach((entry) => {
          const it = entry.item;
          const isEditing = currentEditItem && itemKey(currentEditItem) === itemKey(it);
          const isWeekly = !entry.isMonthly && entry.t.dow !== null;
          const chipLabel = entry.isMonthly ? `[월별] ${it.name}` : isWeekly ? `[매주] ${it.name}` : it.name;
          const chip = el(
            'span',
            'rm-grid-chip' + (it.watched ? ' rm-grid-chip-live' : '') +
              (isEditing ? ' rm-grid-chip-editing' : '') +
              (entry.isMonthly ? ' rm-grid-chip-monthly' : ''),
            chipLabel
          );
          const color = SCOPE_COLORS[it.scope] || '#94a3b8';
          chip.style.background = hexToRgba(color, 0.22);
          chip.style.color = color;
          const whenText = entry.isMonthly
            ? `매월 ${entry.t.dom}일 (요일은 참고 표시용)`
            : `${d.label}요일`;
          chip.title = `${it.scope_label || it.scope} · ${it.name} · ${whenText} ${pad2(h)}:${m}` +
            (it.watched ? ' · 실시간 반영 중' : '') + ' (드래그해서 이동 가능)';
          chip.draggable = true;
          chip.addEventListener('click', () => openEditPanel(it));
          chip.addEventListener('dragstart', (evt) => {
            evt.dataTransfer.setData('text/plain', itemKey(it));
            evt.dataTransfer.effectAllowed = 'move';
            chip.classList.add('rm-dragging');
          });
          chip.addEventListener('dragend', () => {
            chip.classList.remove('rm-dragging');
          });
          td.appendChild(chip);
        });
        const minuteVal = m === '00' ? 0 : 30;
        td.addEventListener('dragover', (evt) => {
          evt.preventDefault();
          evt.dataTransfer.dropEffect = 'move';
          td.classList.add('rm-grid-drop-target');
        });
        td.addEventListener('dragleave', () => {
          td.classList.remove('rm-grid-drop-target');
        });
        td.addEventListener('drop', (evt) => {
          evt.preventDefault();
          td.classList.remove('rm-grid-drop-target');
          handleGridDrop(evt.dataTransfer.getData('text/plain'), d.dow, h, minuteVal);
        });
        row.appendChild(td);
      });
      tbody.appendChild(row);
    }
    table.appendChild(tbody);

    wrapper.appendChild(table);
    return wrapper;
  }

  // 뷰 모드(그리드/타임라인)에 따라 실제 렌더링을 위임하는 진입점.
  function renderActive(items) {
    const container_ = container.querySelector('#rm-timetable');
    const statTotal = container.querySelector('#rm-stat-total');
    const statOverlap = container.querySelector('#rm-stat-overlap');
    if (!container_) return;

    const overlapCount = computeOverlapMap(items); // item._times도 함께 채워짐(두 뷰 공용)

    if (statTotal) statTotal.textContent = String(items.length);
    if (statOverlap) statOverlap.textContent = String(overlapCount);

    container_.innerHTML = '';
    if (viewMode === 'grid') {
      container_.appendChild(renderGridTable(items));
    } else {
      renderTimelineInto(container_, items);
    }
  }

  function applyFilter() {
    const searchInput = container.querySelector('#rm-search-input');
    const query = (searchInput ? searchInput.value : '').trim().toLowerCase();
    const filtered = query
      ? allItems.filter((item) => (item.name || '').toLowerCase().includes(query))
      : allItems;
    renderActive(filtered);
  }

  function setViewMode(mode) {
    if (viewMode === mode) return;
    viewMode = mode;
    const gridBtn = container.querySelector('#rm-view-grid-btn');
    const timelineBtn = container.querySelector('#rm-view-timeline-btn');
    const gridLegend = container.querySelector('#rm-grid-legend');
    const timelineLegend = container.querySelector('#rm-timeline-legend');
    gridBtn.classList.toggle('active', mode === 'grid');
    timelineBtn.classList.toggle('active', mode === 'timeline');
    gridLegend.hidden = mode !== 'grid';
    timelineLegend.hidden = mode !== 'timeline';
    applyFilter();
  }

  // ==================================================================
  // 스케줄 도우미 (편집 패널)
  // ==================================================================
  function dowLabel(dow) {
    const labels = ['일요일', '월요일', '화요일', '수요일', '목요일', '금요일', '토요일'];
    return labels[dow] || `요일(${dow})`;
  }

  // cron 문자열 -> 도우미 폼에 채울 값 추정. 표준 패턴(분 시 * * *),
  // (분 시 * * 요일[,요일...]), (분 시 일 * *)만 도우미로 표현 가능하고, 그 외는 '직접 입력'으로 처리.
  function parseCronToHelper(cronStr) {
    const fields = (cronStr || '').trim().split(/\s+/);
    if (fields.length < 5) return { type: 'manual' };
    const [minute, hour, dom, month, dow] = fields;
    const isNum = (v) => /^\d+$/.test(v);
    if (isNum(minute) && isNum(hour) && month === '*') {
      const hh = pad2(parseInt(hour, 10) % 24);
      const mm = pad2(parseInt(minute, 10) % 60);
      if (dom === '*' && dow === '*') {
        return { type: 'daily', hh, mm };
      }
      if (dom === '*' && /^[0-6](,[0-6])*$/.test(dow)) {
        const dowList = Array.from(new Set(dow.split(','))); // 콤마 구분 다중 요일, 중복 제거
        return { type: 'weekly', hh, mm, dow: dowList };
      }
      if (dow === '*' && /^([1-9]|[12]\d|3[01])$/.test(dom)) {
        return { type: 'monthly', hh, mm, dom };
      }
    }
    return { type: 'manual' };
  }

  function readHelperFields() {
    const dowChecks = Array.from(container.querySelectorAll('#rm-repeat-dow-checks input[type="checkbox"]'));
    const dow = dowChecks.filter((cb) => cb.checked).map((cb) => cb.value); // 선택된 요일 값 배열(예: ['1','3','5'])
    return {
      type: container.querySelector('#rm-repeat-type').value,
      time: container.querySelector('#rm-repeat-time').value || '03:00',
      dow,
      dom: container.querySelector('#rm-repeat-dom').value,
    };
  }

  function buildCronFromHelper() {
    const { type, time, dow, dom } = readHelperFields();
    const [hh, mm] = time.split(':').map((v) => parseInt(v, 10) || 0);
    if (type === 'daily') return `${mm} ${hh} * * *`;
    if (type === 'weekly') {
      // 아무 요일도 선택 안 한 상태에서 저장을 누르면 cron이 깨지므로 '*'로 안전 폴백
      // (그러면 사실상 "매일"과 동일해짐 - buildSummaryText에서 미리 경고를 보여줌).
      const sorted = dow.map((v) => parseInt(v, 10)).sort((a, b) => a - b);
      const dowField = sorted.length > 0 ? sorted.join(',') : '*';
      return `${mm} ${hh} * * ${dowField}`;
    }
    if (type === 'monthly') return `${mm} ${hh} ${dom} * *`;
    return container.querySelector('#rm-cron-text').value.trim();
  }

  function buildSummaryText() {
    const { type, time, dow, dom } = readHelperFields();
    if (type === 'daily') return `매일 ${time} 실행`;
    if (type === 'weekly') {
      if (dow.length === 0) return '⚠ 요일을 하나 이상 선택하세요 (선택 없으면 매일 실행으로 저장됩니다).';
      const labels = dow
        .map((v) => parseInt(v, 10))
        .sort((a, b) => a - b)
        .map((d) => dowLabel(d))
        .join(', ');
      return `매주 ${labels} ${time} 실행`;
    }
    if (type === 'monthly') return `매월 ${dom}일 ${time} 실행 (31일 등 없는 달은 자동으로 건너뜀)`;
    return '직접 입력한 Cron식을 그대로 사용합니다.';
  }

  function updateHelperVisibility() {
    const type = container.querySelector('#rm-repeat-type').value;
    const timeField = container.querySelector('#rm-time-field');
    const dowField = container.querySelector('#rm-dow-field');
    const domField = container.querySelector('#rm-dom-field');
    const cronInput = container.querySelector('#rm-cron-text');
    timeField.style.display = type === 'manual' ? 'none' : '';
    dowField.style.display = type === 'weekly' ? '' : 'none';
    domField.style.display = type === 'monthly' ? '' : 'none';
    cronInput.readOnly = type !== 'manual';
  }

  // 도우미 필드가 바뀔 때마다: cron 텍스트/요약을 갱신하고, 편집 중인 항목의
  // pendingCron을 갱신한 뒤 메인 타임테이블을 즉시 다시 그려 실시간 미리보기.
  function onHelperChanged() {
    updateHelperVisibility();
    const type = readHelperFields().type;
    const cronStr = buildCronFromHelper();

    if (type !== 'manual') {
      container.querySelector('#rm-cron-text').value = cronStr;
    }
    container.querySelector('#rm-helper-summary').textContent =
      `${buildSummaryText()} | Cron: ${cronStr}`;

    if (currentEditItem) {
      currentEditItem._pendingCron = cronStr;
      applyFilter();
    }
  }

  function onCronTextChanged() {
    const type = readHelperFields().type;
    if (type !== 'manual') return;
    const cronStr = container.querySelector('#rm-cron-text').value.trim();
    container.querySelector('#rm-helper-summary').textContent = `직접 입력: ${cronStr || '(비어 있음)'}`;
    if (currentEditItem) {
      currentEditItem._pendingCron = cronStr;
      applyFilter();
    }
  }

  function bindHelperListenersOnce() {
    if (helperListenersBound) return;
    helperListenersBound = true;
    container.querySelector('#rm-repeat-type').addEventListener('change', onHelperChanged);
    container.querySelector('#rm-repeat-time').addEventListener('input', onHelperChanged);
    container.querySelectorAll('#rm-repeat-dow-checks input[type="checkbox"]').forEach((cb) => {
      cb.addEventListener('change', onHelperChanged);
    });
    container.querySelector('#rm-repeat-dom').addEventListener('change', onHelperChanged);
    container.querySelector('#rm-cron-text').addEventListener('input', onCronTextChanged);
    container.querySelector('#rm-edit-close-btn').addEventListener('click', () => closeEditPanel(true));
    container.querySelector('#rm-edit-overlay').addEventListener('click', (evt) => {
      if (evt.target.id === 'rm-edit-overlay') closeEditPanel(true);
    });
    container.querySelector('#rm-edit-save-btn').addEventListener('click', saveEdit);
  }

  function openEditPanel(item) {
    bindHelperListenersOnce();
    currentEditItem = item;
    item._pendingCron = item.cron_schedule;

    container.querySelector('#rm-edit-libname').textContent = `${item.scope_label || item.scope} · ${item.name}`;
    container.querySelector('#rm-save-error').hidden = true;

    const parsed = parseCronToHelper(item.cron_schedule);
    const typeSel = container.querySelector('#rm-repeat-type');
    const timeInput = container.querySelector('#rm-repeat-time');
    const dowChecks = Array.from(container.querySelectorAll('#rm-repeat-dow-checks input[type="checkbox"]'));
    const domSel = container.querySelector('#rm-repeat-dom');
    const cronInput = container.querySelector('#rm-cron-text');

    typeSel.value = parsed.type;
    dowChecks.forEach((cb) => {
      cb.checked = false;
    });
    if (parsed.type === 'daily') {
      timeInput.value = `${parsed.hh}:${parsed.mm}`;
    } else if (parsed.type === 'weekly') {
      timeInput.value = `${parsed.hh}:${parsed.mm}`;
      const selected = new Set(parsed.dow);
      dowChecks.forEach((cb) => {
        cb.checked = selected.has(cb.value);
      });
    } else if (parsed.type === 'monthly') {
      timeInput.value = `${parsed.hh}:${parsed.mm}`;
      domSel.value = String(parseInt(parsed.dom, 10));
    }
    cronInput.value = item.cron_schedule || '';

    updateHelperVisibility();
    container.querySelector('#rm-helper-summary').textContent =
      `${buildSummaryText()} | Cron: ${effectiveCron(item)}`;

    container.querySelector('#rm-edit-overlay').hidden = false;
    applyFilter();
  }

  // 그리드 뷰에서 라이브러리 칩을 다른 칸(요일/시간)에 드롭했을 때 호출.
  // 즉시 저장하지 않고, 편집 패널을 그 위치(매주 특정 요일)로 미리 채운 뒤
  // 미리보기만 반영한다 - 실수 방지를 위해 저장은 사용자가 직접 눌러야 함.
  function handleGridDrop(draggedKey, targetDow, targetHour, targetMinute) {
    if (!draggedKey) return;
    const item = allItems.find((it) => itemKey(it) === draggedKey);
    if (!item) return;

    if (!currentEditItem || itemKey(currentEditItem) !== draggedKey) {
      openEditPanel(item);
    }

    const typeSel = container.querySelector('#rm-repeat-type');
    const timeInput = container.querySelector('#rm-repeat-time');
    const dowChecks = Array.from(container.querySelectorAll('#rm-repeat-dow-checks input[type="checkbox"]'));

    typeSel.value = 'weekly';
    timeInput.value = `${pad2(targetHour)}:${pad2(targetMinute)}`;
    dowChecks.forEach((cb) => {
      cb.checked = cb.value === String(targetDow);
    });

    onHelperChanged(); // pendingCron 재계산 + 요약 갱신 + 타임테이블 실시간 미리보기
    console.log(
      LOG_PREFIX,
      `드래그 이동 미리보기: ${item.name} -> ${DOW_LABELS[targetDow]}요일 ${pad2(targetHour)}:${pad2(targetMinute)} (저장 버튼을 눌러야 확정됨)`
    );
  }

  function closeEditPanel(discardPending) {
    if (currentEditItem && discardPending) {
      delete currentEditItem._pendingCron;
    }
    currentEditItem = null;
    container.querySelector('#rm-edit-overlay').hidden = true;
    applyFilter();
  }

  function saveEdit() {
    if (!currentEditItem) return;
    const cronStr = (currentEditItem._pendingCron || '').trim();
    const errorBox = container.querySelector('#rm-save-error');
    errorBox.hidden = true;

    if (!cronStr || cronStr.split(/\s+/).length < 5) {
      errorBox.textContent = '유효한 5필드 Cron식이 아닙니다 (예: 0 3 * * *).';
      errorBox.hidden = false;
      return;
    }

    const saveBtn = container.querySelector('#rm-edit-save-btn');
    const originalHtml = saveBtn.innerHTML;
    saveBtn.disabled = true;
    saveBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> 저장 중...';

    const item = currentEditItem;
    rpc('update_cron', { scope: item.scope, id: item.id, cron_schedule: cronStr })
      .catch((err) => ({ success: false, error: err.message }))
      .then((data) => {
        if (!data || !data.success) {
          errorBox.textContent = (data && (data.error || data.message)) || '저장에 실패했습니다.';
          errorBox.hidden = false;
          return;
        }
        item.cron_schedule = cronStr;
        delete item._pendingCron;
        console.log(LOG_PREFIX, '저장 완료:', item.name, cronStr);
        closeEditPanel(false);
      })
      .catch((err) => {
        errorBox.textContent = `요청 중 오류: ${err}`;
        errorBox.hidden = false;
      })
      .finally(() => {
        saveBtn.disabled = false;
        saveBtn.innerHTML = originalHtml;
      });
  }

  // ==================================================================
  // 데이터 로딩
  // ==================================================================
  function fetchSchedules() {
    const status = container.querySelector('#rm-status');
    if (status) { status.style.display = 'block'; status.textContent = '스케줄 불러오는 중...'; }
    rpc('schedules').then((data) => {
      allItems = Array.isArray(data.items) ? data.items : [];
      const live = container.querySelector('#rm-stat-live');
      if (live) live.textContent = String(allItems.filter((it) => it.watched).length);
      if (Array.isArray(data.errors) && data.errors.length > 0) {
        status.style.display = 'block';
        status.textContent = `일부 세션을 불러오지 못했습니다: ${data.errors.join(' / ')}`;
      } else if (status) {
        status.style.display = 'none';
      }
      applyFilter();
    }).catch((err) => {
      if (status) { status.style.display = 'block'; status.textContent = '스케줄을 가져오지 못했습니다: ' + err.message; }
    });
  }

  const searchInput = container.querySelector('#rm-search-input');
  if (searchInput) {
    let debounceTimer = null;
    searchInput.addEventListener('input', () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(applyFilter, 200);
    });
  }

  const refreshBtn = container.querySelector('#rm-refresh-btn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', fetchSchedules);
  }

  const viewGridBtn = container.querySelector('#rm-view-grid-btn');
  if (viewGridBtn) {
    viewGridBtn.addEventListener('click', () => setViewMode('grid'));
  }
  const viewTimelineBtn = container.querySelector('#rm-view-timeline-btn');
  if (viewTimelineBtn) {
    viewTimelineBtn.addEventListener('click', () => setViewMode('timeline'));
  }

  window.__gdwScheduleLoad = fetchSchedules;

  })(pane, PID);
})();
