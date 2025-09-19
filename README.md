# duplihere

### What
Copy & Paste finder for source files or any structured utf-8 text files.


### Why

A number of different copy and paste detectors exist.  Some examples include:

* [PMD](https://pmd.github.io/)
* [Simian](http://www.harukizaemon.com/simian/)
* [tctoolkit](https://github.com/nitinbhide/tctoolkit)

So why write another?  I've wanted a simple tool, one that works like simian,
but is open source and free for everyone. Thus this project was born.  In
general I think writing a lexer and tokenizing the source isn't needed.
There is a ton of code that is very much copy and pasted verbatim.
Developers are lazy, they don't change things :-)

### How

```bash
duplihere - 0.9.3 - find duplicate text

usage: duplihere [-pj -l <number> -i <file name> -t <thread number>] -f <pattern or specific file>

Find duplicate lines of text in one or more text files.

The duplicated text can be at different levels of indention,
but otherwise needs to be identical.

More information: https://github.com/tasleson/duplihere

argument:                                        description
    -p, --print                                  print duplicate text [default: false]
    -j, --json                                   output JSON [default: false]
    -l, --lines <number>                         minimum number of duplicate lines [default: 6]
    -f, --file <pattern or specific file>        pattern or file eg. "**/*.[h|c]" recursive, "*.py", "file.ext", can repeat [required]
    -i, --ignore <file name>                     file containing hash values to ignore, one per line
    -t, --threads <thread number>                number of threads to utilize. Set to 0 to match #cpu cores [default: 4]
```

An example where we recurse in a directory for python files and a directory
that contains python files ...

**Unix/Linux/macOS:**
```bash
$ duplihere -l 10 -p -f '/home/user/somewhere/**/*.py' -f '/tmp/*.py'
```

**Windows:**
```cmd
> duplihere -l 10 -p -f "C:\Users\user\somewhere\**\*.py" -f "C:\temp\*.py"
```

An example showing JSON output (not finalized)

**Unix/Linux/macOS:**
```bash
$ duplihere -f /home/tasleson/projects/linux/init/main.c -l 5 -j
```

**Windows:**
```cmd
> duplihere -f "C:\projects\linux\init\main.c" -l 5 -j
```

```json
{
  "num_lines": 5,
  "num_ignored": 0,
  "duplicates": [
    {
      "key": 11558319874972720381,
      "num_lines": 5,
      "files": [
        [
          "/home/tasleson/projects/linux/init/main.c",
          830
        ],
        [
          "/home/tasleson/projects/linux/init/main.c",
          864
        ]
      ]
    }
  ]
}

```

### Installation

This project requires Rust to build and run.

#### Installing Rust

**Windows:**
1. Download and install Rust from [rustup.rs](https://rustup.rs/)
2. Open Command Prompt or PowerShell as Administrator
3. Run the installer and follow the prompts

**Unix/Linux/macOS:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Building from Source

**All platforms:**
```bash
git clone https://github.com/tasleson/duplihere.git
cd duplihere
cargo build --release
```

The executable will be created at:
- **Windows:** `target\release\duplihere.exe`
- **Unix/Linux/macOS:** `target/release/duplihere`

### Status

Tool has enough features and functionality for meaningful results.
With the latest multi-thread support it's quite fast on
big source trees.  Current graph of memory and CPU consumption while examining
the Linux kernel source tree for duplicates. Run against Linux `6.5` branch (~24M lines) and all
available CPU cores. Chart generated with
[psrecord](https://github.com/astrofrog/psrecord).

![threadripper](https://github.com/tasleson/duplihere/assets/2520480/56e59144-e5b0-415c-90f0-b9459006f686)
