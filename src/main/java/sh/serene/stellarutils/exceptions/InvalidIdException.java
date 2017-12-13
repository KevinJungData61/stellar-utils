package sh.serene.stellarutils.exceptions;

import sh.serene.stellarutils.model.epgm.ElementId;

/**
 * Used by Graph Collection Builder. Is thrown when an element ID is referenced that does not exist in the lookup table.
 *
 */
public class InvalidIdException extends RuntimeException {

    public InvalidIdException(String message) {
        super(message);
    }

    public InvalidIdException(ElementId id) {
        super(String.format("Invalid Id: %s", id.toString()));
    }
}
