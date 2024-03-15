# Should Uniffi exported API use the existing structs or should we create seperate structs

We decided to use separate structs because it will allow us to more easily preserve backwards compatibility.
It will also prevent weird stuff in Uniffi from effecting the regular Rust API.

## Seperated structs

pros:

* changes to regular API are shielded
* changes to FFI API won't effect regular API
* can rename by just changing function/method names. Currently renames not supported
      
cons:

* we could forgetting to change the FFI API when regular is updated

## Same structs 

pros:

* code co-location
* less code potentially
* when methods are added the are automatically added to both interfaces
      
cons:

* requirements coming from uniffi would bleed into how we expose regular Rust FFI

