.PHONY: default all build clean compile release shell compileInDocker

rebar='rebar3'
DOCKER_HUB ?= cchalasani
DOCKER_IMAGE ?= erlang_ubuntu:25.1

default: compile
all: clean compile test
compile:
	@$(rebar) compile
clean:
	@$(rebar) clean
cleanall:
	@$(rebar) clean -a
test:
	@$(rebar) do ct
shell:
	@$(rebar) shell
compileInDocker:
	@docker run -v /home:/home -v ${PWD}:/mnt -v "/etc/group:/etc/group:ro" -v "/etc/passwd:/etc/passwd:ro" -v "/etc/shadow:/etc/shadow:ro" -w /mnt -u $$(id -u):$$(id -g) $(DOCKER_HUB)/$(DOCKER_IMAGE) make
