use vergen_gix::{Emitter, GixBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Emit git information
    Emitter::default()
        .add_instructions(&GixBuilder::all_git()?)?
        .emit()?;

    Ok(())
}
