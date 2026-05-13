import ast
import asyncio
import glob
import json
import os
import sys


def _has_top_level_await(code: str) -> bool:
    """Return True if `code` contains top-level `await` / `async for` /
    `async with` outside any function definition. Determined via AST
    walk so we don't false-positive on `await` inside an `async def`."""

    try:
        tree = ast.parse(code, mode="exec", type_comments=False)
    except SyntaxError:
        # The code is syntactically only valid with PyCF_ALLOW_TOP_LEVEL_AWAIT;
        # fall back to True so the async runner takes over and surfaces the
        # real error (if any) from inside asyncio.
        return True

    function_types = (ast.FunctionDef, ast.AsyncFunctionDef)
    for stmt in tree.body:
        for node in ast.walk(stmt):
            # Skip await nodes nested inside a function (they're an
            # `async def` body, not top-level).
            if isinstance(node, function_types) and node is not stmt:
                continue
            if isinstance(node, (ast.Await, ast.AsyncFor, ast.AsyncWith)):
                return True
    return False


def run_notebook(notebook_path):
    print(f"Running {notebook_path}...")
    with open(notebook_path) as f:
        nb = json.load(f)

    code_cells = [cell for cell in nb["cells"] if cell["cell_type"] == "code"]

    # Concatenate code
    code = ""
    for cell in code_cells:
        source = "".join(cell["source"])
        code += source + "\n\n"

    # Execute
    # Adjust cwd to the notebook directory to match its perspective.

    original_cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(os.path.abspath(notebook_path)))

        exec_globals = {}
        if _has_top_level_await(code):
            # Async notebook: compile with the top-level await flag and
            # run the resulting coroutine via asyncio.
            compiled = compile(
                code,
                f"<{os.path.basename(notebook_path)}>",
                "exec",
                flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
            )
            coro = eval(compiled, exec_globals)
            if asyncio.iscoroutine(coro):
                asyncio.run(coro)
        else:
            exec(code, exec_globals)

        print(f"SUCCESS: {notebook_path}")
        return True
    except Exception as e:
        print(f"FAILURE: {notebook_path}")
        print(e)
        import traceback

        traceback.print_exc()
        return False
    finally:
        os.chdir(original_cwd)


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    notebooks = glob.glob(os.path.join(base_dir, "*.ipynb"))

    success = True
    for nb in notebooks:
        if not run_notebook(nb):
            success = False

    if success:
        print("\nAll notebooks ran successfully!")
        sys.exit(0)
    else:
        print("\nSome notebooks failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
