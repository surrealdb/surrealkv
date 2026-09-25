import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';

const wasmPath = process.argv[2];
if (!wasmPath) {
	console.error('Usage: run_wasi.mjs <path-to-wasm>');
	process.exit(1);
}

const wasi = new WASI({
	version: 'preview1',
	args: [wasmPath],
	preopens: {
		'.': '.',
	},
});

const wasmBuffer = await readFile(wasmPath);
const wasm = await WebAssembly.compile(wasmBuffer);
const instance = await WebAssembly.instantiate(wasm, {
	wasi_snapshot_preview1: wasi.wasiImport,
});

wasi.start(instance);
