use std::collections::HashMap;

use uni_cypher::locy_ast::{LocyProgram, UseDecl};

use super::errors::LocyCompileError;

/// Resolved module context: maps short rule names to qualified names.
#[derive(Debug, Clone, Default)]
pub struct ModuleContext {
    /// The current module's qualified name (e.g., "acme.compliance").
    pub module_name: Option<String>,
    /// Imported rule names: short name → qualified module + rule name.
    pub imports: HashMap<String, String>,
}

/// Build a module context from a program's MODULE and USE declarations.
pub fn resolve_modules(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
) -> Result<ModuleContext, LocyCompileError> {
    let mut ctx = ModuleContext::default();

    if let Some(module_decl) = &program.module {
        ctx.module_name = Some(module_decl.name.to_string());
    }

    for use_decl in &program.uses {
        resolve_use(&mut ctx, use_decl, available_modules)?;
    }

    Ok(ctx)
}

fn resolve_use(
    ctx: &mut ModuleContext,
    use_decl: &UseDecl,
    available_modules: &HashMap<String, Vec<String>>,
) -> Result<(), LocyCompileError> {
    let module_name = use_decl.name.to_string();
    if let Some(rules) = available_modules.get(&module_name) {
        match &use_decl.imports {
            None => {
                // Glob import: import all rules from the module
                for rule in rules {
                    ctx.imports
                        .insert(rule.clone(), format!("{}.{}", module_name, rule));
                }
            }
            Some(selected) => {
                // Selective import: only import listed rules
                for name in selected {
                    if rules.contains(name) {
                        ctx.imports
                            .insert(name.clone(), format!("{}.{}", module_name, name));
                    } else {
                        return Err(LocyCompileError::ImportNotFound {
                            module: module_name,
                            rule: name.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    } else {
        Err(LocyCompileError::ModuleNotFound { name: module_name })
    }
}

/// Resolve a rule name using the module context.
/// Returns the canonical name (qualified if from an import, otherwise as-is).
pub fn resolve_rule_name(ctx: &ModuleContext, name: &str) -> String {
    if let Some(qualified) = ctx.imports.get(name) {
        qualified.clone()
    } else if let Some(module) = &ctx.module_name {
        // If no dot in name and we have a module, qualify it
        if !name.contains('.') {
            format!("{}.{}", module, name)
        } else {
            name.to_string()
        }
    } else {
        name.to_string()
    }
}
