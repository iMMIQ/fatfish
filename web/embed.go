package web

import "embed"

// Assets embeds static files (htmx.min.js) and HTML templates for the :6185 admin console.
//
//go:embed static/* templates/*
var Assets embed.FS
