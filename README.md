# 技术笔记博客

使用 [Hugo](https://gohugo.io/) + [Ananke 主题](https://github.com/theNewDynamic/gohugo-theme-ananke) 搭建的静态博客。

## 本地开发

### 前置要求

- 安装 Hugo (extended 版本)

### 启动本地服务器

```bash

$ git submodule update --init --recursive
$ docker pull jakejarvis/hugo-extended:latest
$ docker run -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest server --buildDrafts --buildFuture --bind 0.0.0.0
```

启动后访问 http://localhost:1313/blog/

### 常用命令

| 命令 | 说明 |
|------|------|
| `hugo server -D` | 启动开发服务器，包含草稿 |
| `hugo server --disableFastRender` | 完整重新渲染模式 |
| `hugo --minify` | 构建（压缩版） |
| `hugo new content posts/文章名.md` | 创建新文章 |

## 创建新文章

```bash
# 创建新文章
~/.local/bin/hugo new content posts/my-new-post.md

# 编辑 content/posts/my-new-post.md
```

文章模板：

```markdown
+++
date = '2026-04-12T18:18:09+08:00'
draft = false
title = '文章标题'
tags = ['标签1', '标签2']
+++

文章内容...
```

## 部署

推送到 main 分支后，GitHub Actions 会自动构建并部署到 GitHub Pages。

## 目录结构

```
.
├── archetypes/      # 文章模板
├── assets/          # 资源文件
├── content/         # 文章内容
├── data/            # 数据文件
├── layouts/         # HTML 模板
├── static/          # 静态文件
├── themes/ananke    # 主题
└── hugo.toml        # 站点配置
```
