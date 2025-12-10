import os, sys, time, psutil, random, string, shutil, threading, platform, json, ctypes, ctypes.wintypes
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ==============================================================
# ========================= CONFIG ==============================
# ==============================================================

#Мод копирования
#0	Ничего не копируем.
#1	Копируем структуру каталогов, файлы не копируем.
#2	Копируем каталоги и файлы с сохранением структуры.
#3	Копируем только файлы, без структуры (в плоскую папку).
MODE = 2

#Копировать только файлы с данными расширениями (да/нет)
COPY_ONLY_SELECTED_TYPES = True

#расширения которые файлов, которые копируем (если COPY_ONLY_SELECTED_TYPES = True)
ALLOWED_EXTENSIONS = {".jpeg", ".jpg", ".heic", ".png"}

#Максимальный размер файлов, которые копируем (в мегабайтах) (0 - без ограничений)
MAX_FILE_SIZE_MB = 0

#создать .json файл со структурой каталогов и файлов внешнего носителя (да/нет)
CREATE_JSON = True

#список системных каталогов внешнего носителя, которые игнорируем
IGNORE_DIRS = {
    "System Volume Information",
    "MSOCache"
}
# ==============================================================


def base_dir():
    return os.path.dirname(sys.executable if getattr(sys, "frozen", False)
                           else os.path.abspath(__file__))

BASE = base_dir()
CATALOGS = os.path.join(BASE, "catalogs")
os.makedirs(CATALOGS, exist_ok=True)


def win_label(path):
    p = path.rstrip("\\/")
    for _ in range(3):
        try:
            vol = ctypes.create_unicode_buffer(1024)
            fs = ctypes.create_unicode_buffer(1024)
            s = ctypes.wintypes.DWORD()
            m = ctypes.wintypes.DWORD()
            f = ctypes.wintypes.DWORD()
            ok = ctypes.windll.kernel32.GetVolumeInformationW(
                p + "\\", vol, 1024, ctypes.byref(s),
                ctypes.byref(m), ctypes.byref(f), fs, 1024
            )
            if ok and vol.value.strip():
                return vol.value.strip()
        except:
            pass
        time.sleep(0.2)
    letter = p.replace(":", "").replace("\\", "")
    return f"Disk{letter}"

def get_label(path):
    if platform.system() == "Windows":
        lab = win_label(path)
        return lab.replace(" ", "_")
    name = os.path.basename(path.rstrip("/"))
    return name.replace(" ", "_") or "NO_LABEL"


def make_target_dir(label):
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d_%H-%M-%S")
    timestamp_us = int(time.time() * 1_000_000)
    rnd = "".join(random.choice(string.digits) for _ in range(4))
    folder = f"{ts}_{timestamp_us}_{rnd}_{label}"
    path = os.path.join(CATALOGS, folder)
    os.makedirs(path, exist_ok=True)
    return path, folder


def scan_incremental(root, folder_name, json_done_event):
    struct = {"path": root, "folders": [], "files": []}
    json_root = os.path.join(CATALOGS, folder_name)
    last = 0
    counter = 0
    prev_tmp = None

    def walk(path, out):
        nonlocal last, counter, prev_tmp
        try:
            entries = os.scandir(path)
        except:
            return
        for e in entries:
            name = e.name
            if name.startswith(".") or name in IGNORE_DIRS:
                continue
            full = e.path
            if e.is_dir(follow_symlinks=False):
                sub = {"path": full, "folders": [], "files": []}
                out["folders"].append(sub)
                walk(full, sub)
            elif e.is_file():
                out["files"].append(name)
            if time.time() - last > 1:
                last = time.time()
                counter += 1
                tmp_path = os.path.join(json_root, f"{folder_name}_{counter}.json")
                try:
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        json.dump(struct, f, ensure_ascii=False, indent=2)
                except:
                    pass
                if prev_tmp and os.path.exists(prev_tmp):
                    try: os.remove(prev_tmp)
                    except: pass
                prev_tmp = tmp_path
            if not os.path.exists(root):
                return

    walk(root, struct)

    try:
        if prev_tmp and os.path.exists(prev_tmp):
            os.remove(prev_tmp)
    except:
        pass

    final_path = os.path.join(json_root, f"{folder_name}.json")
    try:
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(struct, f, ensure_ascii=False, indent=2)
    except:
        pass

    json_done_event.set()


def start_json_scan(root, folder_name, json_done_event):
    threading.Thread(target=scan_incremental, args=(root, folder_name, json_done_event), daemon=True).start()


def file_allowed(path):
    if MODE in (0, 1):
        return False
    ext = os.path.splitext(path)[1].lower()
    if COPY_ONLY_SELECTED_TYPES and ext not in ALLOWED_EXTENSIONS:
        return False
    if MAX_FILE_SIZE_MB > 0:
        try:
            if os.path.getsize(path) > MAX_FILE_SIZE_MB * 1024 * 1024:
                return False
        except:
            return False
    return True


def resolve_conflict(path):
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    timestamp_us = int(time.time() * 1_000_000)
    return f"{base}_{timestamp_us}{ext}"


active = set()
lock = threading.Lock()


def copy_file_safe(src, dst):
    dst = resolve_conflict(dst)
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
    except:
        pass


def copy_device(mount, label):
    target_dir, folder_name = make_target_dir(label)
    json_done_event = threading.Event()

    if CREATE_JSON:
        start_json_scan(mount, folder_name, json_done_event)

    if MODE == 0:
        json_done_event.wait()
        open(os.path.join(target_dir, "complete.txt"), "w").close()
        with lock:
            active.discard(mount)
        return

    executor = ThreadPoolExecutor(max_workers=(os.cpu_count() or 4) * 4)

    try:
        for root, dirs, files in os.walk(mount, topdown=True):
            dirs[:] = [d for d in dirs if not d.startswith(".") and d not in IGNORE_DIRS]
            rel = os.path.relpath(root, mount)

            if MODE in (1, 2):
                dest_root = target_dir if rel == "." else os.path.join(target_dir, rel)
                try: os.makedirs(dest_root, exist_ok=True)
                except: pass

            if MODE in (2, 3):
                for f in files:
                    if f.startswith(".") or f in IGNORE_DIRS:
                        continue
                    src = os.path.join(root, f)
                    if not file_allowed(src):
                        continue
                    if MODE == 2:
                        dst = os.path.join(dest_root, f)
                    else:
                        dst = os.path.join(target_dir, f)
                    executor.submit(copy_file_safe, src, dst)

            if not os.path.exists(mount):
                break
    finally:
        executor.shutdown(wait=True)
        if CREATE_JSON:
            json_done_event.wait()
        try:
            open(os.path.join(target_dir, "complete.txt"), "w").close()
        except:
            pass
        with lock:
            active.discard(mount)


def start_copy(mount):
    label = get_label(mount)
    with lock:
        if mount in active:
            return
        active.add(mount)
    threading.Thread(target=copy_device, args=(mount, label), daemon=True).start()


def list_disks():
    out = []
    for p in psutil.disk_partitions(all=True):
        mp = p.mountpoint
        opts = p.opts.lower()
        if "removable" in opts:
            out.append(mp); continue
        if mp.startswith("/media") or mp.startswith("/run/media") or mp.startswith("/Volumes"):
            out.append(mp); continue
        if platform.system() == "Windows":
            if len(mp.replace("\\", "")) == 2:
                out.append(mp)
    return out


def main():
    seen = set()
    initial_local = set()

    for d in list_disks():
        seen.add(d)
        opts = ""
        for p in psutil.disk_partitions(all=True):
            if p.mountpoint == d:
                opts = p.opts.lower()
        if "removable" in opts:
            start_copy(d)
        else:
            initial_local.add(d)

    while True:
        time.sleep(1)
        current = set(list_disks())
        new = current - seen
        for d in new:
            removable = False
            opts = ""
            for p in psutil.disk_partitions(all=True):
                if p.mountpoint == d:
                    opts = p.opts.lower()
                    if "removable" in opts:
                        removable = True
            if removable or d not in initial_local:
                start_copy(d)

        removed = seen - current
        for d in removed:
            if d in initial_local:
                initial_local.remove(d)

        seen = current


if __name__ == "__main__":
    main()
