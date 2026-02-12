import { Registry } from "../src/registry.ts";

export default await Registry.fromDirectory(
  new URL("./tasks", import.meta.url),
);
