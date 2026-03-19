build: build-nix build-win build-mac

build-nix:
	GOOS=linux GOARCH=amd64 go build -o builds/amd64-linux-accounting main.go
	GOOS=linux GOARCH=arm64 go build -o builds/arm64-linux-accounting main.go
	GOOS=linux GOARCH=386 go build -o builds/i386-linux-accounting main.go

build-win:
	GOOS=windows GOARCH=amd64 go build -o builds/amd64-win-accounting.exe main.go
	GOOS=windows GOARCH=arm64 go build -o builds/arm64-win-accounting.exe main.go
	GOOS=windows GOARCH=386 go build -o builds/i386-win-accounting.exe main.go

build-mac:
	GOOS=darwin GOARCH=amd64 go build -o builds/amd64-darwin-accounting main.go
	GOOS=darwin GOARCH=arm64 go build -o builds/arm64-darwin-accounting main.go