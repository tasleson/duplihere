#!/usr/bin/python3

import hashlib

from sys import argv, exit

ROLLING_LINE_SIZE = 6


def file_signatures(fn):
    file_sigs = []

    with open(fn) as h:
        for l in h:
            m = hashlib.md5()
            m.update(str.encode(l.strip()))
            file_sigs.append((m.hexdigest()))

    return file_sigs


def rolling_hashes(collision_hash, file_name, file_sigs):
    num_lines = len(file_sigs)
    if num_lines > ROLLING_LINE_SIZE:
        for i, l in enumerate(file_sigs[:-ROLLING_LINE_SIZE]):
            m = hashlib.md5()
            for n in range(i, i + ROLLING_LINE_SIZE):
                m.update(str.encode(file_sigs[n]))
            dig = m.hexdigest()
            collision_hash.setdefault(dig, []).append((file_name, i))


def process_file(collision_hash, file_hashes, file_name):
    real_name = file_name       # Replace with realpath(file_name)?
    try:
        if real_name not in file_hashes:
            file_hashes[real_name] = file_signatures(real_name)
            rolling_hashes(collision_hash, real_name, file_hashes[real_name])
    except UnicodeDecodeError as e:
        print("File not apparently utf-8 %s %s" % (real_name, str(e)))
        exit(1)


def walk_collision(file_hashes, left_file, left_start, right_file, right_start):
    offset = 0
    hash_txt = ""

    l_h = file_hashes[left_file]
    r_h = file_hashes[right_file]

    # Avoid matching the entire file or a good portion of it.
    if l_h == r_h and left_start == right_start:
        return None

    try:
        while True:
            # We may run off the end of the list, but we will let it happen
            if l_h[left_start + offset] == r_h[right_start + offset]:
                hash_txt += l_h[left_start + offset]
                offset += 1
            else:
                break
    except IndexError:
        pass

    if offset > ROLLING_LINE_SIZE:
        # Returning hash signature by hashing the hashes, otherwise it could
        # become huge.
        m = hashlib.md5()
        m.update(str.encode(hash_txt))
        return m.hexdigest(), offset, ((left_file, left_start),
                                       (right_file, right_start))
    return None


def find_collisions(collision_hash, file_hashes):

    def chunk_sig(result_list):
        m = hashlib.md5()
        num_lines = result_list[0][0]
        for i in result_list:
            fn, starts = i[1]
            end = starts + 1 + num_lines
            m.update(str.encode("%s%s" % (str(end), fn)))

        return m.hexdigest()

    def print_dup_text(filename, start, end):
        with open(filename) as fh:
            lines = fh.readlines()

        for i in range(start, start + end):
            print("%s" % lines[i].rstrip())

    def cmp_overall(a):
        k = "%010d%010d%s" % (a[0][0], a[0][1][1], a[0][1][0])
        return k

    def cmp_line(a):
        return "%010d%s" % (a[1][1], a[1][0])

    def print_report(rd):
        rd.sort(key=cmp_overall)

        num_lines = 0

        for entries in rd:
            print("*" * 80)
            print("Found %d copy & pasted lines in the following files:" %
                  entries[0][0])

            num_lines += entries[0][0] * len(entries)

            for specific_file in entries:
                num_duplicates = specific_file[0]
                file_name, start_line = specific_file[1]
                end_line = start_line + 1 + num_duplicates

                l_file, l_start = specific_file[1]
                print("Between lines %d and %d in %s" %
                      (l_start + 1, (end_line), l_file))

            print_dup_text(entries[0][1][0], entries[0][1][1], entries[0][0])

        print("Found %d duplicate lines in %d chunks" % (num_lines, len(rd)))

    results_hash = {}

    for collisions in collision_hash.values():
        if len(collisions) > 1:
            for x, l in enumerate(collisions[:-1]):
                for r in collisions[x + 1:]:
                    max_collision = walk_collision(
                        file_hashes, l[0], l[1], r[0], r[1])
                    if max_collision:
                        key, num, value = max_collision
                        results_hash.setdefault(key, []).append((num, value[0]))
                        results_hash.setdefault(key, []).append((num, value[1]))

    res = sorted(results_hash.values(), key=lambda index: index[0][0],
                 reverse=True)

    chunks_processed = {}
    final_report = []
    for results in res:
        # Collision comparison can result in duplicates, remove them.  This is
        # caused because for larger sections of copy & pasted txt we can get
        # multiple rolling hash entries.  When the source tree has many
        # duplicates this tool really shows its issues.
        culled = list(set(results))
        culled.sort(key=cmp_line)

        collision_sig = chunk_sig(culled)
        if collision_sig not in chunks_processed:
            chunks_processed[collision_sig] = True
            final_report.append(culled)

    print_report(final_report)


if __name__ == "__main__":
    g_collision_hash = {}
    g_file_hashes = {}

    for f in argv[1:]:
        process_file(g_collision_hash, g_file_hashes, f)

    find_collisions(g_collision_hash, g_file_hashes)
