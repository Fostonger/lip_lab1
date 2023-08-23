CFLAGS=--std=c17
BUILDDIR=build
SRCDIR=src
C=gcc

all: $(BUILDDIR)/util.o $(BUILDDIR)/database_manager.o $(BUILDDIR)/table.o $(BUILDDIR)/data.o $(BUILDDIR)/data_iterator.o $(BUILDDIR)/main.o
	$(C) -o $(BUILDDIR)/main $^

build:
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/util.o: $(SRCDIR)/util.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@

$(BUILDDIR)/database_manager.o: $(SRCDIR)/database_manager.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@

$(BUILDDIR)/table.o: $(SRCDIR)/table.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@

$(BUILDDIR)/data.o: $(SRCDIR)/data.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@

$(BUILDDIR)/data_iterator.o: $(SRCDIR)/data_iterator.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@

$(BUILDDIR)/main.o: $(SRCDIR)/main.c $(BUILDDIR)
	$(C) -c $(CFLAGS) $< -o $@


clean:
	rm -rf $(BUILDDIR)
