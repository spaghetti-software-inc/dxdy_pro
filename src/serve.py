from textual_serve.server import Server

if __name__ == "__main__":
    server = Server("python -m textual run app.py",
                    #host = "192.168.193.51",
                    #port = 8000,
                    title="dxdy")
    server.serve()
