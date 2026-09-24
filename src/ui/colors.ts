/**
 * Lightweight ANSI color and styling utilities for terminal UI rendering.
 */

export const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  italic: "\x1b[3m",
  underline: "\x1b[4m",
  inverse: "\x1b[7m",
  hidden: "\x1b[8m",

  // Foreground colors
  black: "\x1b[30m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  white: "\x1b[37m",
  gray: "\x1b[90m",

  // Background colors
  bgBlack: "\x1b[40m",
  bgRed: "\x1b[41m",
  bgGreen: "\x1b[42m",
  bgYellow: "\x1b[43m",
  bgBlue: "\x1b[44m",
  bgMagenta: "\x1b[45m",
  bgCyan: "\x1b[46m",
  bgWhite: "\x1b[47m",
  bgGray: "\x1b[100m",

  // Screen / cursor controls
  enterAltScreen: "\x1b[?1049h",
  exitAltScreen: "\x1b[?1049l",
  hideCursor: "\x1b[?25l",
  showCursor: "\x1b[?25h",
  clearScreen: "\x1b[2J",
  cursorHome: "\x1b[H",
  clearLine: "\x1b[2K",
};

export function bold(text: string): string {
  return `${ANSI.bold}${text}${ANSI.reset}`;
}

export function dim(text: string): string {
  return `${ANSI.dim}${text}${ANSI.reset}`;
}

export function cyan(text: string): string {
  return `${ANSI.cyan}${text}${ANSI.reset}`;
}

export function yellow(text: string): string {
  return `${ANSI.yellow}${text}${ANSI.reset}`;
}

export function green(text: string): string {
  return `${ANSI.green}${text}${ANSI.reset}`;
}

export function magenta(text: string): string {
  return `${ANSI.magenta}${text}${ANSI.reset}`;
}

export function red(text: string): string {
  return `${ANSI.red}${text}${ANSI.reset}`;
}

export function gray(text: string): string {
  return `${ANSI.gray}${text}${ANSI.reset}`;
}

export function inverse(text: string): string {
  return `${ANSI.inverse}${text}${ANSI.reset}`;
}

export function bgBlue(text: string): string {
  return `${ANSI.bgBlue}${ANSI.white}${text}${ANSI.reset}`;
}

export function bgGray(text: string): string {
  return `${ANSI.bgGray}${ANSI.white}${text}${ANSI.reset}`;
}

/**
 * Strips ANSI escape sequences from a string to measure its visible character length.
 */
export function stripAnsi(str: string): string {
  // eslint-disable-next-line no-control-regex
  return str.replace(/\x1b\[[0-9;]*m/g, "").replace(/\x1b\[\?[0-9]+[hl]/g, "");
}

/**
 * Truncates string to a max visible width, adding an ellipsis if needed.
 */
export function truncateVisible(str: string, maxWidth: number): string {
  if (maxWidth <= 0) return "";
  const plain = stripAnsi(str);
  if (plain.length <= maxWidth) return str;
  if (maxWidth <= 3) return plain.slice(0, maxWidth);
  return `${plain.slice(0, maxWidth - 1)}…`;
}
