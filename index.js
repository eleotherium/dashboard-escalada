// Compatibilidade browser-only: nao usar require() aqui.
// Se este arquivo for carregado por algum template antigo, ele nao quebra a pagina.
if (typeof window !== "undefined" && typeof window.require !== "function") {
  window.require = function () { return {}; };
}
window.__ENV = window.__ENV || {};
