.PHONY: setup activate dev prod

VENV := venv

setup:
	@if [ ! -d "$(VENV)" ]; then \
		echo "📦 criando venv..."; \
		python3 -m venv $(VENV); \
	else \
		echo "📦 venv já existe"; \
	fi
	@echo "⚡ instalando requirements..."
	@./$(VENV)/bin/pip install --upgrade pip
	@if [ -f "requirements.txt" ]; then \
		./$(VENV)/bin/pip install -r requirements.txt; \
	else \
		echo "⚠️ requirements.txt não encontrado"; \
	fi
	@echo "✅ setup finalizado."
