ERLC=erlc
ERL=erl
DHT=dht.erl
CONTROL=control.erl

all: compile run

compile:
	$(ERLC) $(DHT)
	$(ERLC) $(CONTROL)

run:
	$(ERL) -noshell -s control start -s init stop

clean:
	rm -f *.beam
