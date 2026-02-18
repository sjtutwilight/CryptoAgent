#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_REMOTE="origin"
DEFAULT_MAIN="main"

usage() {
  cat <<'USAGE'
用法:
  ./tool/git-branch.sh new <branch> [--remote origin] [--main main]
  ./tool/git-branch.sh sync-main [--remote origin] [--main main]
  ./tool/git-branch.sh merge-main [--remote origin] [--main main] [--ff-only]
  ./tool/git-branch.sh help

命令说明:
  new         同步主干并基于主干创建新分支，然后切换到该分支
  sync-main   拉取并快进更新本地主干
  merge-main  将最新主干合并到当前分支
USAGE
}

die() {
  echo "错误: $*" >&2
  exit 1
}

require_git_repo() {
  git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "当前目录不是 git 仓库"
}

require_clean_worktree() {
  if [[ -n "$(git -C "$ROOT_DIR" status --porcelain)" ]]; then
    die "检测到未提交改动，请先提交/暂存后再执行"
  fi
}

parse_common_flags() {
  REMOTE="$DEFAULT_REMOTE"
  MAIN_BRANCH="$DEFAULT_MAIN"
  FF_ONLY=false
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote)
        shift
        [[ $# -gt 0 ]] || die "--remote 缺少参数"
        REMOTE="$1"
        ;;
      --main)
        shift
        [[ $# -gt 0 ]] || die "--main 缺少参数"
        MAIN_BRANCH="$1"
        ;;
      --ff-only)
        FF_ONLY=true
        ;;
      *)
        die "未知参数: $1"
        ;;
    esac
    shift
  done
}

sync_main() {
  git -C "$ROOT_DIR" fetch "$REMOTE" "$MAIN_BRANCH"
  git -C "$ROOT_DIR" switch "$MAIN_BRANCH"
  git -C "$ROOT_DIR" pull --ff-only "$REMOTE" "$MAIN_BRANCH"
}

cmd_new() {
  local branch="$1"
  [[ -n "$branch" ]] || die "new 需要分支名"
  shift
  parse_common_flags "$@"
  require_clean_worktree
  sync_main
  git -C "$ROOT_DIR" switch -c "$branch"
  echo "已创建并切换到分支: $branch (基于 $REMOTE/$MAIN_BRANCH)"
}

cmd_sync_main() {
  parse_common_flags "$@"
  require_clean_worktree
  sync_main
  echo "主干已更新: $MAIN_BRANCH <= $REMOTE/$MAIN_BRANCH"
}

cmd_merge_main() {
  parse_common_flags "$@"
  require_clean_worktree
  git -C "$ROOT_DIR" fetch "$REMOTE" "$MAIN_BRANCH"
  local current
  current="$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD)"
  if [[ "$current" == "$MAIN_BRANCH" ]]; then
    die "当前就在 $MAIN_BRANCH，请切换到功能分支再执行 merge-main"
  fi
  if [[ "$FF_ONLY" == "true" ]]; then
    git -C "$ROOT_DIR" merge --ff-only "$REMOTE/$MAIN_BRANCH"
  else
    git -C "$ROOT_DIR" merge "$REMOTE/$MAIN_BRANCH"
  fi
  echo "已将 $REMOTE/$MAIN_BRANCH 合并到当前分支: $current"
}

main() {
  require_git_repo
  local cmd="${1:-help}"
  shift || true
  case "$cmd" in
    new)
      [[ $# -ge 1 ]] || die "new 需要分支名"
      cmd_new "$@"
      ;;
    sync-main)
      cmd_sync_main "$@"
      ;;
    merge-main)
      cmd_merge_main "$@"
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      die "未知命令: $cmd"
      ;;
  esac
}

main "$@"
