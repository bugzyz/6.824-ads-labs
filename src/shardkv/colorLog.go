package shardkv

import (
	"fmt"
	"time"
)

//get\putAppend log
const print = 1

//Migrateion/configuration/cleanup log
const print1 = 1

//snapshot log
const print2 = 1

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
	color_magenta //洋红

	info = "[INFO-shardkv]"
	trac = "[TRAC-shardkv]"
	erro = "[ERRO-shardkv]"
	warn = "[WARN-shardkv]"
	succ = "[SUCC-shardkv]"
)

// see complete color rules in document in https://en.wikipedia.org/wiki/ANSI_escape_code#cite_note-ecma48-13
func Trace(format string, a ...interface{}) {
	if print < 0 {
		return
	}
	prefix := yellow(trac)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Info(format string, a ...interface{}) {
	if print < 0 {
		return
	}
	prefix := blue(info)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Success(format string, a ...interface{}) {
	if print < 0 {
		return
	}
	prefix := green(succ)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Warning(format string, a ...interface{}) {
	if print < 0 {
		return
	}
	prefix := magenta(warn)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Error(format string, a ...interface{}) {
	if print < 0 {
		return
	}
	prefix := red(erro)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}

func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}

func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}

func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}

func magenta(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_magenta, s)
}

func formatLog(prefix string) string {
	return time.Now().Format("2006/01/02 15:04:05") + " " + prefix + " "
}

func Trace1(format string, a ...interface{}) {
	if print1 < 0 {
		return
	}
	prefix := yellow(trac)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Info1(format string, a ...interface{}) {
	if print1 < 0 {
		return
	}
	prefix := blue(info)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Success1(format string, a ...interface{}) {
	if print1 < 0 {
		return
	}
	prefix := green(succ)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Warning1(format string, a ...interface{}) {
	if print1 < 0 {
		return
	}
	prefix := magenta(warn)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Error1(format string, a ...interface{}) {
	if print1 < 0 {
		return
	}
	prefix := red(erro)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Trace2(format string, a ...interface{}) {
	if print2 < 0 {
		return
	}
	prefix := yellow(trac)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Info2(format string, a ...interface{}) {
	if print2 < 0 {
		return
	}
	prefix := blue(info)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Success2(format string, a ...interface{}) {
	if print2 < 0 {
		return
	}
	prefix := green(succ)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Warning2(format string, a ...interface{}) {
	if print2 < 0 {
		return
	}
	prefix := magenta(warn)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}

func Error2(format string, a ...interface{}) {
	if print2 < 0 {
		return
	}
	prefix := red(erro)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
